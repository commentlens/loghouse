package chunkio

import (
	"bufio"
	"bytes"
	"sync"
)

const (
	readerBufferSize = 1024 * 1024 * 10
)

var readerPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, readerBufferSize)
	},
}

func NewBuffer() *bufio.Reader {
	return readerPool.Get().(*bufio.Reader)
}

func RecycleBuffer(buf *bufio.Reader) {
	buf.Reset(nil)
	readerPool.Put(buf)
}

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func newBuffer() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func recycleBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufPool.Put(buf)
}
