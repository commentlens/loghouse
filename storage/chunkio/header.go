package chunkio

import (
	"bytes"
	"errors"
	"io"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/tlv"
)

type Header struct {
	OffsetStart uint64
	Size        uint64
	Labels      map[string]string
	Start       time.Time
	End         time.Time
	Compression string
}

func MatchHeader(hdr *Header, opts *storage.ReadOptions) bool {
	if !storage.MatchLabels(hdr.Labels, opts.Labels) {
		return false
	}
	if !opts.Start.IsZero() && !hdr.End.IsZero() && opts.Start.After(hdr.End) {
		return false
	}
	if !opts.End.IsZero() && !hdr.Start.IsZero() && opts.End.Before(hdr.Start) {
		return false
	}
	return true
}

func WriteHeader(w io.Writer, hdr *Header) error {
	b, err := encodeHeader(hdr)
	if err != nil {
		return err
	}
	tw := tlv.NewWriter(w)
	return tw.Write(tlvTypeHeader, b)
}

func ReadHeader(r io.Reader) (*Header, error) {
	tr := tlv.NewReader(r)
	typ, val, err := tr.Read()
	if err != nil {
		return nil, err
	}
	if typ != tlvTypeHeader {
		return nil, ErrUnexpectedTLVType
	}
	return decodeHeader(val)
}

func encodeHeader(hdr *Header) ([]byte, error) {
	buf := new(bytes.Buffer)

	err := encodeUint64(buf, tlvTypeOffsetStart, hdr.OffsetStart)
	if err != nil {
		return nil, err
	}
	err = encodeUint64(buf, tlvTypeSize, hdr.Size)
	if err != nil {
		return nil, err
	}
	if len(hdr.Labels) > 0 {
		err := encodeMap(buf, tlvTypeLabels, hdr.Labels)
		if err != nil {
			return nil, err
		}
	}
	if !hdr.Start.IsZero() {
		err := encodeTime(buf, tlvTypeStart, hdr.Start)
		if err != nil {
			return nil, err
		}
	}
	if !hdr.End.IsZero() {
		err := encodeTime(buf, tlvTypeEnd, hdr.End)
		if err != nil {
			return nil, err
		}
	}
	if hdr.Compression != "" {
		err := encodeString(buf, tlvTypeCompression, "s2")
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeHeader(val io.Reader) (*Header, error) {
	var hdr Header
	tr := tlv.NewReader(val)
	for {
		typ, val, err := tr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		switch typ {
		case tlvTypeOffsetStart:
			n, err := decodeUint64(val)
			if err != nil {
				return nil, err
			}
			hdr.OffsetStart = n
		case tlvTypeSize:
			n, err := decodeUint64(val)
			if err != nil {
				return nil, err
			}
			hdr.Size = n
		case tlvTypeLabels:
			m, err := decodeMap(val)
			if err != nil {
				return nil, err
			}
			hdr.Labels = m
		case tlvTypeStart:
			t, err := decodeTime(val)
			if err != nil {
				return nil, err
			}
			hdr.Start = t
		case tlvTypeEnd:
			t, err := decodeTime(val)
			if err != nil {
				return nil, err
			}
			hdr.End = t
		case tlvTypeCompression:
			s, err := decodeString(val)
			if err != nil {
				return nil, err
			}
			hdr.Compression = s
		default:
			return nil, ErrUnexpectedTLVType
		}
	}
	return &hdr, nil
}
