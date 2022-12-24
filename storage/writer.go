package storage

type Writer interface {
	Write([]*LogEntry) error
}
