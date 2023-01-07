package tlv

import (
	"encoding/binary"
	"io"
)

type Reader interface {
	Read() (uint64, *io.SectionReader, error)
}

type reader struct {
	r   io.ReaderAt
	off int64
	b   []byte
}

func NewReader(r io.ReaderAt) Reader {
	return &reader{
		r: r,
		b: make([]byte, 8),
	}
}

func (r *reader) Read() (uint64, *io.SectionReader, error) {
	typ, err := r.readUint64()
	if err != nil {
		return 0, nil, err
	}
	l, err := r.readUint64()
	if err != nil {
		return 0, nil, err
	}
	val := io.NewSectionReader(r.r, r.off, int64(l))
	r.off += int64(l)
	return typ, val, nil
}

func (r *reader) readUint64() (uint64, error) {
	_, err := r.r.ReadAt(r.b[:1], r.off)
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
		_, err := r.r.ReadAt(b, r.off)
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
