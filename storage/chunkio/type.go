package chunkio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"time"
	"unsafe"

	"github.com/commentlens/loghouse/storage/tlv"
)

var (
	ErrUnexpectedTLVType = errors.New("unexpected tlv type")
)

const (
	tlvTypeString = iota + 1
	tlvTypeHeader
	tlvTypeOffsetStart
	tlvTypeSize
	tlvTypeLabels
	tlvTypeStart
	tlvTypeEnd
	tlvTypeCompression
	tlvTypeCount
	tlvTypeIndex
)

func encodeString(w io.Writer, typ uint64, s string) error {
	return tlv.NewWriter(w).Write(typ, *(*[]byte)(unsafe.Pointer(&s)))
}

func decodeString(val io.Reader) (string, error) {
	buf := newBuffer()
	defer recycleBuffer(buf)
	_, err := buf.ReadFrom(val)
	if err != nil {
		return "", err
	}
	b := buf.Bytes()
	return string(b), nil
}

func encodeUint64(w io.Writer, typ uint64, n uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return tlv.NewWriter(w).Write(typ, b[:])
}

func decodeUint64(val io.Reader) (uint64, error) {
	buf := newBuffer()
	defer recycleBuffer(buf)
	_, err := buf.ReadFrom(val)
	if err != nil {
		return 0, err
	}
	b := buf.Bytes()
	return binary.BigEndian.Uint64(b), nil
}

func encodeTime(w io.Writer, typ uint64, t time.Time) error {
	return encodeUint64(w, typ, uint64(t.UnixMilli()))
}

func decodeTime(val io.Reader) (time.Time, error) {
	n, err := decodeUint64(val)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(int64(n)).UTC(), nil
}

func encodeMap(w io.Writer, typ uint64, m map[string]string) error {
	buf := new(bytes.Buffer)
	for k, v := range m {
		for _, s := range []string{k, v} {
			err := encodeString(buf, tlvTypeString, s)
			if err != nil {
				return err
			}
		}
	}
	return tlv.NewWriter(w).Write(typ, buf.Bytes())
}

func decodeMap(val io.Reader) (map[string]string, error) {
	tr := tlv.NewReader(val)
	m := make(map[string]string)
	for {
		typKey, valKey, err := tr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if typKey != tlvTypeString {
			return nil, ErrUnexpectedTLVType
		}
		k, err := decodeString(valKey)
		if err != nil {
			return nil, err
		}
		typVal, valVal, err := tr.Read()
		if err != nil {
			return nil, err
		}
		if typVal != tlvTypeString {
			return nil, ErrUnexpectedTLVType
		}
		v, err := decodeString(valVal)
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}
