package tlv

import (
	"encoding/binary"
	"io"
	"math"
)

type Writer interface {
	Write(uint64, []byte) error
}

type writer struct {
	w io.Writer
}

func NewWriter(w io.Writer) Writer {
	return &writer{
		w: w,
	}
}

func (w *writer) Write(typ uint64, val []byte) error {
	err := w.writeUint64(typ)
	if err != nil {
		return err
	}
	err = w.writeUint64(uint64(len(val)))
	if err != nil {
		return err
	}
	_, err = w.w.Write(val)
	if err != nil {
		return err
	}
	return nil
}

func (w *writer) writeUint64(v uint64) error {
	var b [9]byte
	var n int
	switch {
	case v > math.MaxUint32:
		b[0] = 0xFF
		binary.BigEndian.PutUint64(b[1:], v)
		n = 9
	case v > math.MaxUint16:
		b[0] = 0xFE
		binary.BigEndian.PutUint32(b[1:], uint32(v))
		n = 5
	case v > math.MaxUint8-3:
		b[0] = 0xFD
		binary.BigEndian.PutUint16(b[1:], uint16(v))
		n = 3
	default:
		b[0] = uint8(v)
		n = 1
	}
	_, err := w.w.Write(b[:n])
	return err
}
