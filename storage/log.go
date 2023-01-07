package storage

import (
	"encoding/json"
	"time"
)

type LogEntry struct {
	Labels map[string]string
	Time   time.Time
	Data   json.RawMessage
}
