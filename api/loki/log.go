package loki

import (
	"net/http"

	"github.com/sirupsen/logrus"
)

func httpLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			logrus.WithField("method", r.Method).WithField("url", r.URL.String()).Info("loki http request served")
			next.ServeHTTP(w, r)
		},
	)
}
