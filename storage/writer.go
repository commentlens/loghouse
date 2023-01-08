package storage

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
)

type Writer interface {
	Write([]*LogEntry) error
}

func HashLabels(labels map[string]string) (string, error) {
	var keys []string
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var tuples []string
	for _, k := range keys {
		v := labels[k]
		tuples = append(tuples, k, v)
	}

	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(tuples, "_"))))
	return digest, nil
}
