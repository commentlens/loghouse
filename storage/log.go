package storage

import (
	"encoding/json"
	"time"

	"github.com/tidwall/gjson"
)

type LogEntry struct {
	Labels map[string]string
	Time   time.Time
	Data   json.RawMessage
}

var (
	_ json.Unmarshaler = (*LogEntry)(nil)
)

func (e *LogEntry) UnmarshalJSON(b []byte) error {
	v := gjson.ParseBytes(b)
	labels := make(map[string]string)
	for k, v := range v.Get("Labels").Map() {
		labels[k] = v.Str
	}
	e.Labels = labels
	e.Time = v.Get("Time").Time()
	e.Data = json.RawMessage(v.Get("Data").Raw)
	return nil
}
