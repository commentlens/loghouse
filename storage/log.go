package storage

import (
	"bytes"
	"time"
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
