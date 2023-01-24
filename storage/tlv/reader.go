package tlv

import (
	"encoding/binary"
	"io"
)

type Reader interface {
	Read() (uint64, io.Reader, error)
	ReadSection() (uint64, uint64, error)
}

type reader struct {
	r   io.Reader
	b   [8]byte
	off uint64
}

func NewReader(r io.Reader) Reader {
	return &reader{
		r: r,
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
	val := io.LimitReader(r.r, int64(l))
	return typ, val, nil
}

func (r *reader) ReadSection() (uint64, uint64, error) {
	off := r.off
	_, err := r.readUint64()
	if err != nil {
		return 0, 0, err
	}
	l, err := r.readUint64()
	if err != nil {
		return 0, 0, err
	}
	_, err = io.CopyN(io.Discard, r.r, int64(l))
	if err != nil {
		return 0, 0, err
	}
	r.off += l
	return off, r.off - off, nil
}

func (r *reader) readUint64() (uint64, error) {
	_, err := io.ReadFull(r.r, r.b[:1])
	if err != nil {
		return 0, err
	}
	r.off += 1
	var n uint64
	switch r.b[0] {
	case 0xFF:
		n = 8
	case 0xFE:
		n = 4
	case 0xFD:
		n = 2
	}
	if n > 0 {
		_, err := io.ReadFull(r.r, r.b[:n])
		if err != nil {
			return 0, err
		}
		r.off += n
	}
	switch n {
	case 8:
		return binary.BigEndian.Uint64(r.b[:n]), nil
	case 4:
		return uint64(binary.BigEndian.Uint32(r.b[:n])), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(r.b[:n])), nil
	default:
		return uint64(r.b[0]), nil
	}
}
