package loki

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/davecgh/go-spew/spew"
	"github.com/julienschmidt/httprouter"
)

func NewServer(opts *ServerOptions) http.Handler {
	m := httprouter.New()
	m.GET("/loki/api/v1/query", opts.query)
	m.GET("/loki/api/v1/query_range", opts.queryRange)
	m.GET("/loki/api/v1/labels", opts.labels)
	m.GET("/loki/api/v1/label/:name/values", opts.labelValues)
	m.POST("/loki/api/v1/push", opts.push)
	return m
}

type ServerOptions struct {
	StorageReader storage.Reader
	StorageWriter storage.Writer
}

type QueryResponse struct {
	Status string            `json:"status"`
	Data   QueryResponseData `json:"data"`
}

type QueryResponseData struct {
	ResultType string    `json:"resultType"`
	Result     []*Stream `json:"result"`
}

type Stream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

// https://grafana.com/docs/loki/latest/api/#query-loki
func (opts *ServerOptions) query(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	json.NewEncoder(rw).Encode(QueryResponse{
		Status: "success",
		Data: QueryResponseData{
			ResultType: "streams",
			Result: []*Stream{{
				Stream: map[string]string{"app": "test"},
				Values: [][]string{
					{fmt.Sprint(time.Now().Add(-10 * time.Minute).UnixNano()), "test"},
					{fmt.Sprint(time.Now().Add(-8 * time.Minute).UnixNano()), "test1"},
					{fmt.Sprint(time.Now().Add(-6 * time.Minute).UnixNano()), "test2"},
				},
			}},
		},
	})
}

// https://grafana.com/docs/loki/latest/api/#query-loki-over-a-range-of-time
func (opts *ServerOptions) queryRange(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	json.NewEncoder(rw).Encode(QueryResponse{
		Status: "success",
		Data: QueryResponseData{
			ResultType: "streams",
			Result: []*Stream{{
				Stream: map[string]string{"app": "test"},
				Values: [][]string{
					{fmt.Sprint(time.Now().Add(-10 * time.Minute).UnixNano()), "test"},
					{fmt.Sprint(time.Now().Add(-8 * time.Minute).UnixNano()), "test1"},
					{fmt.Sprint(time.Now().Add(-6 * time.Minute).UnixNano()), "test2"},
				},
			}},
		},
	})
}

type LabelResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// https://grafana.com/docs/loki/latest/api/#list-labels-within-a-range-of-time
func (opts *ServerOptions) labels(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	json.NewEncoder(rw).Encode(LabelResponse{
		Status: "success",
		Data:   []string{"app", "app2"},
	})
}

// https://grafana.com/docs/loki/latest/api/#list-label-values-within-a-range-of-time
func (opts *ServerOptions) labelValues(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	json.NewEncoder(rw).Encode(LabelResponse{
		Status: "success",
		Data:   []string{"test", "test1"},
	})
}

type PushRequest struct {
	Streams []*Stream `json:"streams"`
}

// https://grafana.com/docs/loki/latest/api/#push-log-entries-to-loki
func (opts *ServerOptions) push(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var pr PushRequest
	json.NewDecoder(r.Body).Decode(&pr)
	spew.Dump(r.Header, pr)
}
