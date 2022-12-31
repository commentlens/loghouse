package loki

import (
	"net/http"

	"github.com/felixge/httpsnoop"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

func httpLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			reqID := ulid.Make().String()
			m := httpsnoop.CaptureMetrics(next, w, r)
			log := logrus.
				WithField("method", r.Method).
				WithField("url", r.URL.String()).
				WithField("req_id", reqID).
				WithField("code", m.Code).
				WithField("duration_seconds", m.Duration.Seconds()).
				WithField("bytes_written", m.Written)
			log.Info("loki http request done")
		},
	)
}
