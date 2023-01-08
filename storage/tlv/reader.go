package tlv

import (
	"encoding/binary"
	"io"
)

type Reader interface {
	Read() (uint64, io.Reader, error)
}

type reader struct {
	r   io.Reader
	off int64
	b   []byte
}

func NewReader(r io.Reader) Reader {
	return &reader{
		r: r,
		b: make([]byte, 8),
	}
}

func (r *reader) Read() (uint64, io.Reader, error) {
	typ, err := r.readUint64()
	if err != nil {
		return 0, nil, err
	}
	l, err := r.readUint64()
	if err != nil {
		return 0, nil, err
	}
	var val io.Reader
	if ra, ok := r.r.(io.ReaderAt); ok {
		val = io.NewSectionReader(ra, r.off, int64(l))
	} else {
		val = io.LimitReader(r.r, int64(l))
	}
	r.off += int64(l)
	return typ, val, nil
}

func (r *reader) read(b []byte) (int, error) {
	if ra, ok := r.r.(io.ReaderAt); ok {
		return ra.ReadAt(b, r.off)
	}
	return r.r.Read(b)
}

func (r *reader) readUint64() (uint64, error) {
	_, err := r.read(r.b[:1])
	if err != nil {
		return 0, err
	}
	r.off += 1
	var n int64
	switch r.b[0] {
	case 0xFF:
		n = 8
	case 0xFE:
		n = 4
	case 0xFD:
		n = 2
	}
	b := r.b[:n]
	if len(b) > 0 {
		_, err := r.read(b)
		if err != nil {
			return 0, err
		}
		r.off += n
	}
	switch n {
	case 8:
		return binary.BigEndian.Uint64(b), nil
	case 4:
		return uint64(binary.BigEndian.Uint32(b)), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(b)), nil
	default:
		return uint64(r.b[0]), nil
	}
}
