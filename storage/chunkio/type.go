package chunkio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"time"

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
)

func readAll(val io.Reader) ([]byte, error) {
	return val.(tlv.Valuer).ReadAll()
}

func encodeString(s string) ([]byte, error) {
	return []byte(s), nil
}

func decodeString(val io.Reader) (string, error) {
	b, err := readAll(val)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func encodeUint64(n uint64) ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b, nil
}

func decodeUint64(val io.Reader) (uint64, error) {
	b, err := readAll(val)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func encodeTime(t time.Time) ([]byte, error) {
	return encodeUint64(uint64(t.UnixMilli()))
}

func decodeTime(val io.Reader) (time.Time, error) {
	n, err := decodeUint64(val)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(int64(n)).UTC(), nil
}

func encodeMap(m map[string]string) ([]byte, error) {
	buf := new(bytes.Buffer)
	tw := tlv.NewWriter(buf)
	for k, v := range m {
		for _, s := range []string{k, v} {
			b, err := encodeString(s)
			if err != nil {
				return nil, err
			}
			err = tw.Write(tlvTypeString, b)
			if err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
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
