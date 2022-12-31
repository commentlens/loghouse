package loki

import (
	"net/http"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

func httpLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			reqID := ulid.Make().String()
			log := logrus.WithField("method", r.Method).WithField("url", r.URL.String()).WithField("req_id", reqID)
			log.Info("loki http request started")
			defer log.Info("loki http request finished")
			next.ServeHTTP(w, r)
		},
	)
}
