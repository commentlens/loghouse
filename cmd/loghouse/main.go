package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/commentlens/loghouse/api/loki"
	"github.com/commentlens/loghouse/storage/filesystem"
	"github.com/commentlens/loghouse/storage/label"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	log := logrus.StandardLogger()
	log.Info("started")
	defer log.Info("stopped")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
	}()

	r := filesystem.NewCompactReader()
	w := filesystem.NewCompactWriter()
	srv := &http.Server{Addr: ":3100", Handler: loki.NewServer(&loki.ServerOptions{
		StorageReader: r,
		StorageWriter: w,
		LabelStore:    label.NewStore(1000),
	})}
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return w.BackgroundCompact(ctx)
	})
	g.Go(func() error {
		return srv.ListenAndServe()
	})
	g.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(context.Background())
	})
	err := g.Wait()
	if err != nil {
		log.WithError(err).Warn("stopped with error")
	}
}
