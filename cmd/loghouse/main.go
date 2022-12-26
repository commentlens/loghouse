package main

import (
	"net/http"

	"github.com/commentlens/loghouse/api/loki"
	"github.com/commentlens/loghouse/storage/filesystem"
)

func main() {
	srv := loki.NewServer(&loki.ServerOptions{
		StorageReader: filesystem.NewCompactReader(),
		StorageWriter: filesystem.NewCompactWriter(),
	})
	http.ListenAndServe(":3100", srv)
}
