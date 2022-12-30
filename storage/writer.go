package storage

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
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

	b, err := json.Marshal(tuples)
	if err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(b))
	return digest, nil
}
