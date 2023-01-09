package tlv

import (
	"encoding/binary"
	"io"
)

type Reader interface {
	Read() (uint64, io.ReadCloser, error)
}

type reader struct {
	r io.Reader
	b []byte
}

func NewReader(r io.Reader) Reader {
	return &reader{
		r: r,
		b: make([]byte, 8),
	}
}

type Valuer struct {
	r *io.LimitedReader
}

type peeker interface {
	Peek(int) ([]byte, error)
}

func (v *Valuer) Read(b []byte) (int, error) {
	return v.r.Read(b)
}

func (v *Valuer) Peek(n int) ([]byte, error) {
	return v.r.R.(peeker).Peek(n)
}

func (v *Valuer) ReadAll() ([]byte, error) {
	defer v.Close()

	return v.Peek(int(v.r.N))
}

func (v *Valuer) Close() error {
	_, err := io.Copy(io.Discard, v.r)
	return err
}

func (r *reader) Read() (uint64, io.ReadCloser, error) {
	typ, err := r.readUint64()
	if err != nil {
		return 0, nil, err
	}
	l, err := r.readUint64()
	if err != nil {
		return 0, nil, err
	}
	val := &Valuer{r: &io.LimitedReader{R: r.r, N: int64(l)}}
	return typ, val, nil
}

func (r *reader) readUint64() (uint64, error) {
	_, err := io.ReadFull(r.r, r.b[:1])
	if err != nil {
		return 0, err
	}
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
		_, err := io.ReadFull(r.r, b)
		if err != nil {
			return 0, err
		}
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
