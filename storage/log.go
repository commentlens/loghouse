package storage

import (
	"bytes"
	"sort"
	"time"

	"github.com/tidwall/gjson"
)

type LogSummary struct {
	Labels map[string]string
	Start  time.Time
	End    time.Time
	Count  uint64
}

type LogEntry struct {
	Labels map[string]string
	Time   time.Time
	Data   LogEntryData
}

type LogEntryData []byte

func (m LogEntryData) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return m, nil
}

func (m *LogEntryData) UnmarshalJSON(data []byte) error {
	if !(bytes.HasPrefix(data, []byte{'{'}) && bytes.HasSuffix(data, []byte{'}'})) {
		data = nil
	}
	*m = append((*m)[0:0], data...)
	return nil
}

func (m *LogEntryData) Values() ([]string, error) {
	tm := make(map[string]struct{})
	var itr func(k, v gjson.Result) bool
	itr = func(k, v gjson.Result) bool {
		if k.Str != "" {
			tm[k.Str] = struct{}{}
		}
		switch v.Type {
		case gjson.True:
			tm[v.Raw] = struct{}{}
		case gjson.False:
			tm[v.Raw] = struct{}{}
		case gjson.String:
			if gjson.Valid(v.Str) {
				gjson.Parse(v.Str).ForEach(itr)
			} else {
				tm[v.Str] = struct{}{}
			}
		case gjson.Number:
			tm[v.Raw] = struct{}{}
		case gjson.Null:
			tm[v.Raw] = struct{}{}
		case gjson.JSON:
			v.ForEach(itr)
		}
		return true
	}
	gjson.ParseBytes(*m).ForEach(itr)
	var ts []string
	for t := range tm {
		ts = append(ts, t)
	}
	sort.Strings(ts)
	return ts, nil
}
