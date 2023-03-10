package loki

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/filesystem"
	"github.com/commentlens/loghouse/storage/label"
	"github.com/julienschmidt/httprouter"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

const (
	ReadLimit       = 100
	ReadRange       = time.Hour
	ReadStep        = 15 * time.Minute
	ReadConcurrency = 100

	TailInterval = 15 * time.Second
	TailDelay    = 5 * time.Second
)

type ServerOptions struct {
	StorageWriter storage.Writer
	LabelStore    *label.Store
}

func NewServer(opts *ServerOptions) http.Handler {
	m := httprouter.New()
	m.GET("/loki/api/v1/query", opts.query)
	m.GET("/loki/api/v1/query_range", opts.queryRange)
	m.GET("/loki/api/v1/tail", opts.tail)
	m.GET("/loki/api/v1/labels", opts.labels)
	m.GET("/loki/api/v1/label/:name/values", opts.labelValues)
	m.GET("/loki/api/v1/series", opts.series)
	m.POST("/loki/api/v1/push", opts.push)
	return httpLogMiddleware(m)
}

type ErrorResponse struct {
	Message string `json:"message"`
}

type QueryResponse struct {
	Status string            `json:"status"`
	Data   QueryResponseData `json:"data"`
}

type QueryResponseData struct {
	ResultType string      `json:"resultType"`
	Result     interface{} `json:"result"`
}

type Matrix struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}

type Stream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func parseRange(query url.Values) (time.Time, time.Time, error) {
	end := time.Now()
	start := end.Add(-ReadRange)
	if startNsec := query.Get("start"); startNsec != "" {
		nsec, err := strconv.ParseUint(startNsec, 10, 64)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		start = time.Unix(0, int64(nsec))
	}
	if endNsec := query.Get("end"); endNsec != "" {
		nsec, err := strconv.ParseUint(endNsec, 10, 64)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		end = time.Unix(0, int64(nsec))
	}
	return start, end, nil
}

// https://grafana.com/docs/loki/latest/api/#query-loki
func (opts *ServerOptions) query(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	opts.queryRange(rw, r, ps)
}

// https://grafana.com/docs/loki/latest/api/#query-loki-over-a-range-of-time
func (opts *ServerOptions) queryRange(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	result, err := func() (interface{}, error) {
		query := r.URL.Query()
		start, end, err := parseRange(query)
		if err != nil {
			return nil, err
		}
		readStep := ReadStep
		if step := query.Get("step"); step != "" {
			d, err := time.ParseDuration(step)
			if err != nil {
				return nil, err
			}
			readStep = d
		}
		if !(start.Before(end) && readStep > 0) {
			return nil, nil
		}
		isHistogram, err := logqlIsHistogram(query.Get("query"))
		if err != nil {
			return nil, err
		}
		if isHistogram {
			histogramSize := end.Sub(start)/readStep + 1
			histogram := make([]uint64, histogramSize)
			mu := make([]sync.Mutex, histogramSize)
			err := logqlRead(ctx, filesystem.NewCompactReader(&filesystem.CompactReaderOptions{
				ReaderCount: ReadConcurrency,
				Reverse:     false,
			}), &storage.ReadOptions{
				Start: start,
				End:   end,
				SummaryFunc: func(s storage.LogSummary) bool {
					if s.Start.IsZero() && s.End.IsZero() {
						return true
					}
					diff := s.End.Sub(s.Start)
					if diff <= 0 {
						return false
					}
					addCount := func(t time.Time, i time.Duration) {
						if !(0 <= i && i < histogramSize) {
							return
						}
						hs := start.Add(i * readStep)
						he := start.Add((i + 1) * readStep)
						if t.After(hs) {
							hs = t
						}
						if t.Add(readStep).Before(he) {
							he = t.Add(readStep)
						}
						if s.End.Before(he) {
							he = s.End
						}
						hdiff := he.Sub(hs)
						if hdiff <= 0 {
							return
						}
						count := float64(s.Count)
						scale := hdiff.Seconds() / diff.Seconds()
						if scale < 1 {
							count *= scale
						}
						mu[i].Lock()
						histogram[i] += uint64(count)
						mu[i].Unlock()
					}
					for t := s.Start; t.Before(s.End); t = t.Add(readStep) {
						i := t.Sub(start) / readStep
						addCount(t, i)
						addCount(t, i+1)
					}
					return false
				},
				FilterFunc: func(e storage.LogEntry) bool {
					i := e.Time.Sub(start) / readStep
					mu[i].Lock()
					histogram[i] += 1
					mu[i].Unlock()
					return false
				},
				ResultFunc: func(e storage.LogEntry) {},
			}, query.Get("query"))
			if err != nil {
				return nil, err
			}
			var values [][]interface{}
			for i, count := range histogram {
				if count > 0 {
					values = append(values, []interface{}{
						start.Add(time.Duration(i) * readStep).Unix(),
						fmt.Sprint(count),
					})
				}
			}
			return []*Matrix{{
				Values: values,
			}}, nil
		}
		var readLimit uint64 = ReadLimit
		if limit := query.Get("limit"); limit != "" {
			n, err := strconv.ParseUint(limit, 10, 64)
			if err != nil {
				return nil, err
			}
			readLimit = n
		}
		reverse := query.Get("direction") == "backward"
		var es []storage.LogEntry
		var mu sync.Mutex
		err = logqlRead(ctx, filesystem.NewCompactReader(&filesystem.CompactReaderOptions{
			ReaderCount: ReadConcurrency,
			Reverse:     reverse,
		}), &storage.ReadOptions{
			Start: start,
			End:   end,
			ResultFunc: func(e storage.LogEntry) {
				mu.Lock()
				defer mu.Unlock()

				if uint64(len(es)) < readLimit {
					es = append(es, e)
				} else {
					cancel()
				}
			},
		}, query.Get("query"))
		if err != nil && !errors.Is(err, context.Canceled) {
			return nil, err
		}
		return createStreams(es)
	}()
	var data QueryResponseData
	switch result := result.(type) {
	case []*Stream:
		data.ResultType = "streams"
		data.Result = result
	case []*Matrix:
		data.ResultType = "matrix"
		data.Result = result
	default:
		rw.WriteHeader(http.StatusBadRequest)
		if err != nil {
			json.NewEncoder(rw).Encode(ErrorResponse{
				Message: err.Error(),
			})
		}
		return
	}
	json.NewEncoder(rw).Encode(QueryResponse{
		Status: "success",
		Data:   data,
	})
}

func createStreams(es []storage.LogEntry) ([]*Stream, error) {
	m := make(map[string][]storage.LogEntry)
	for _, e := range es {
		h, err := storage.HashLabels(e.Labels)
		if err != nil {
			return nil, err
		}
		m[h] = append(m[h], e)
	}
	var streams []*Stream
	for _, es := range m {
		var values [][]string
		for _, e := range es {
			values = append(values, []string{
				fmt.Sprint(e.Time.UnixNano()),
				string(e.Data),
			})
		}
		streams = append(streams, &Stream{
			Stream: es[0].Labels,
			Values: values,
		})
	}
	return streams, nil
}

type TailResponse struct {
	Streams []*Stream `json:"streams"`
}

// https://grafana.com/docs/loki/latest/api/#stream-log-messages
func (opts *ServerOptions) tail(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	conn, err := websocket.Accept(rw, r, nil)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	defer conn.Close(websocket.StatusInternalError, "tail done")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go func() {
		defer cancel()
		for {
			_, _, err := conn.Read(ctx)
			if err != nil {
				return
			}
		}
	}()

	func() error {
		query := r.URL.Query()

		end := time.Now().Add(-TailDelay)
		start := end.Add(-TailInterval)

		ticker := time.NewTicker(TailInterval)
		defer ticker.Stop()

		for {
			var es []storage.LogEntry
			var mu sync.Mutex
			err := logqlRead(ctx, filesystem.NewCompactReader(&filesystem.CompactReaderOptions{
				ReaderCount: ReadConcurrency,
				Reverse:     false,
			}), &storage.ReadOptions{
				Start: start,
				End:   end,
				ResultFunc: func(e storage.LogEntry) {
					mu.Lock()
					defer mu.Unlock()
					es = append(es, e)
				},
			}, query.Get("query"))
			if err != nil {
				return err
			}
			streams, err := createStreams(es)
			if err != nil {
				return err
			}
			err = wsjson.Write(ctx, conn, &TailResponse{
				Streams: streams,
			})
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case t := <-ticker.C:
				start = end
				end = t.Add(-TailDelay)
			}
		}
	}()
}

type LabelResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// https://grafana.com/docs/loki/latest/api/#list-labels-within-a-range-of-time
func (opts *ServerOptions) labels(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	labels, _ := func() ([]string, error) {
		return opts.LabelStore.Labels(), nil
	}()
	json.NewEncoder(rw).Encode(LabelResponse{
		Status: "success",
		Data:   labels,
	})
}

// https://grafana.com/docs/loki/latest/api/#list-label-values-within-a-range-of-time
func (opts *ServerOptions) labelValues(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	labelValues, _ := func() ([]string, error) {
		label := ps.ByName("name")
		return opts.LabelStore.LabelValues(label), nil
	}()
	json.NewEncoder(rw).Encode(LabelResponse{
		Status: "success",
		Data:   labelValues,
	})
}

type SeriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

// https://grafana.com/docs/loki/latest/api/#list-series
func (opts *ServerOptions) series(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	kvs, _ := func() ([]map[string]string, error) {
		var kvs []map[string]string
		for _, k := range opts.LabelStore.Labels() {
			for _, v := range opts.LabelStore.LabelValues(k) {
				kvs = append(kvs, map[string]string{k: v})
			}
		}
		return kvs, nil
	}()
	json.NewEncoder(rw).Encode(SeriesResponse{
		Status: "success",
		Data:   kvs,
	})
}

type PushRequest struct {
	Streams []*Stream `json:"streams"`
}

// https://grafana.com/docs/loki/latest/api/#push-log-entries-to-loki
func (opts *ServerOptions) push(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	err := func() error {
		var pr PushRequest
		err := json.NewDecoder(r.Body).Decode(&pr)
		if err != nil {
			return err
		}
		for _, stream := range pr.Streams {
			for k, v := range stream.Stream {
				opts.LabelStore.Add(k, v)
			}
			var es []storage.LogEntry
			for _, v := range stream.Values {
				if len(v) != 2 {
					continue
				}
				nsec, err := strconv.ParseUint(v[0], 10, 64)
				if err != nil {
					return err
				}
				es = append(es, storage.LogEntry{
					Labels: stream.Stream,
					Time:   time.Unix(0, int64(nsec)),
					Data:   storage.LogEntryData(v[1]),
				})
			}
			err := opts.StorageWriter.Write(es)
			if err != nil {
				return err
			}
		}
		return nil
	}()
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
}
