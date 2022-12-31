package loki

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/commentlens/loghouse/api/loki/logql/lexer"
	"github.com/commentlens/loghouse/api/loki/logql/parser"
	"github.com/commentlens/loghouse/storage"
	"github.com/davecgh/go-spew/spew"
	"github.com/julienschmidt/httprouter"
)

const (
	ReadLimit = 1000
	ReadRange = time.Hour
)

type ServerOptions struct {
	StorageReader storage.Reader
	StorageWriter storage.Writer
}

func NewServer(opts *ServerOptions) http.Handler {
	m := httprouter.New()
	m.GET("/loki/api/v1/query", opts.query)
	m.GET("/loki/api/v1/query_range", opts.queryRange)
	m.GET("/loki/api/v1/labels", opts.labels)
	m.GET("/loki/api/v1/label/:name/values", opts.labelValues)
	m.GET("/loki/api/v1/series", opts.series)
	m.POST("/loki/api/v1/push", opts.push)
	return httpLogMiddleware(m)
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
	result, _ := func() (interface{}, error) {
		query := r.URL.Query()
		start, end, err := parseRange(query)
		if err != nil {
			return nil, err
		}
		expr := query.Get("query")
		lex := lexer.New([]rune(expr))
		q, errs := parser.Parse(lex)
		if len(errs) > 0 {
			spew.Dump(errs)
			return nil, fmt.Errorf("logql: parse query %q", expr)
		}
		if q.IsAmbiguous() {
			q.ReportAmbiguous()
			return nil, fmt.Errorf("logql: ambiguous query %q", expr)
		}
		es, isHistogram, err := logqlRead(opts.StorageReader, func() *storage.ReadOptions {
			return &storage.ReadOptions{
				Start: start,
				End:   end,
				Limit: ReadLimit,
			}
		}, q.GetRoot())
		if err != nil {
			return nil, err
		}
		if isHistogram {
			step, err := time.ParseDuration(query.Get("step"))
			if err != nil {
				return nil, err
			}
			var values [][]interface{}
			i := 0
			for t := start; t.Before(end); t = t.Add(step) {
				var count uint64
				for ; i < len(es); i++ {
					if es[i].Time.Before(t) {
						continue
					}
					if es[i].Time.Before(t.Add(step)) {
						count++
						continue
					}
					break
				}
				values = append(values, []interface{}{
					t.Unix(),
					fmt.Sprint(count),
				})
			}
			return []*Matrix{{
				Values: values,
			}}, nil
		}
		if query.Get("direction") == "backward" {
			reverse(es)
		}
		m := make(map[string][]*storage.LogEntry)
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
		return
	}
	json.NewEncoder(rw).Encode(QueryResponse{
		Status: "success",
		Data:   data,
	})
}

func reverse(s []*storage.LogEntry) {
	for i := 0; i < len(s)/2; i++ {
		j := len(s) - i - 1
		s[i], s[j] = s[j], s[i]
	}
}

type LabelResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// https://grafana.com/docs/loki/latest/api/#list-labels-within-a-range-of-time
func (opts *ServerOptions) labels(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	labels, _ := func() ([]string, error) {
		query := r.URL.Query()
		start, end, err := parseRange(query)
		if err != nil {
			return nil, err
		}
		es, err := opts.StorageReader.Read(&storage.ReadOptions{
			Start: start,
			End:   end,
			Limit: ReadLimit,
		})
		if err != nil {
			return nil, err
		}
		m := make(map[string]struct{})
		for _, e := range es {
			for k := range e.Labels {
				m[k] = struct{}{}
			}
		}
		var labels []string
		for k := range m {
			labels = append(labels, k)
		}
		sort.Strings(labels)
		return labels, nil
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
		query := r.URL.Query()
		start, end, err := parseRange(query)
		if err != nil {
			return nil, err
		}
		es, err := opts.StorageReader.Read(&storage.ReadOptions{
			Start: start,
			End:   end,
			Limit: ReadLimit,
		})
		if err != nil {
			return nil, err
		}
		m := make(map[string]struct{})
		for _, e := range es {
			if v, ok := e.Labels[label]; ok {
				m[v] = struct{}{}
			}
		}
		var labelValues []string
		for k := range m {
			labelValues = append(labelValues, k)
		}
		sort.Strings(labelValues)
		return labelValues, nil
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
		query := r.URL.Query()
		start, end, err := parseRange(query)
		if err != nil {
			return nil, err
		}
		es, err := opts.StorageReader.Read(&storage.ReadOptions{
			Start: start,
			End:   end,
			Limit: ReadLimit,
		})
		if err != nil {
			return nil, err
		}
		m := make(map[string]map[string]struct{})
		for _, e := range es {
			for k, v := range e.Labels {
				if _, ok := m[k]; !ok {
					m[k] = make(map[string]struct{})
				}
				m[k][v] = struct{}{}
			}
		}
		var kvs []map[string]string
		for k, kv := range m {
			for v := range kv {
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
			var es []*storage.LogEntry
			for _, v := range stream.Values {
				if len(v) != 2 {
					continue
				}
				nsec, err := strconv.ParseUint(v[0], 10, 64)
				if err != nil {
					return err
				}
				es = append(es, &storage.LogEntry{
					Labels: stream.Stream,
					Time:   time.Unix(0, int64(nsec)),
					Data:   json.RawMessage(v[1]),
				})
			}
			err = opts.StorageWriter.Write(es)
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
	rw.WriteHeader(http.StatusOK)
}
