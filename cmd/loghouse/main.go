package main

import (
	"context"
	"net/http"

	"github.com/commentlens/loghouse/api/loki"
	"github.com/commentlens/loghouse/storage/filesystem"
)

func main() {
	r := filesystem.NewCompactReader()
	w := filesystem.NewCompactWriter()
	go w.BackgroundCompact(context.Background())

	srv := loki.NewServer(&loki.ServerOptions{
		StorageReader: r,
		StorageWriter: w,
	})
	http.ListenAndServe(":3100", srv)
}
