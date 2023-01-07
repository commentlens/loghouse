package storage

import (
	"bytes"
	"time"
)

type LogEntry struct {
	Labels map[string]string
	Time   time.Time
	Data   LogEntryData
}

type LogEntryData string

func (m LogEntryData) MarshalJSON() ([]byte, error) {
	if m == "" {
		return []byte("null"), nil
	}
	return []byte(m), nil
}

func (m *LogEntryData) UnmarshalJSON(data []byte) error {
	if !bytes.HasPrefix(data, []byte{'{'}) && bytes.HasSuffix(data, []byte{'}'}) {
		return nil
	}
	*m = LogEntryData(data)
	return nil
}
