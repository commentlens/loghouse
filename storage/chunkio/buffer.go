package chunkio

import (
	"bufio"
	"sync"
)

const (
	readerBufferSize = 1024 * 1024 * 10
)

var bufPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, readerBufferSize)
	},
}

func NewBuffer() *bufio.Reader {
	return bufPool.Get().(*bufio.Reader)
}

func RecycleBuffer(buf *bufio.Reader) {
	buf.Reset(nil)
	bufPool.Put(buf)
}
