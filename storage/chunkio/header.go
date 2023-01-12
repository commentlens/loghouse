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

	tw := tlv.NewWriter(buf)
	offsetStart, err := encodeUint64(hdr.OffsetStart)
	if err != nil {
		return nil, err
	}
	err = tw.Write(tlvTypeOffsetStart, offsetStart)
	if err != nil {
		return nil, err
	}
	size, err := encodeUint64(hdr.Size)
	if err != nil {
		return nil, err
	}
	err = tw.Write(tlvTypeSize, size)
	if err != nil {
		return nil, err
	}
	if len(hdr.Labels) > 0 {
		labels, err := encodeMap(hdr.Labels)
		if err != nil {
			return nil, err
		}
		err = tw.Write(tlvTypeLabels, labels)
		if err != nil {
			return nil, err
		}
	}
	if !hdr.Start.IsZero() {
		start, err := encodeTime(hdr.Start)
		if err != nil {
			return nil, err
		}
		err = tw.Write(tlvTypeStart, start)
		if err != nil {
			return nil, err
		}
	}
	if !hdr.End.IsZero() {
		end, err := encodeTime(hdr.End)
		if err != nil {
			return nil, err
		}
		err = tw.Write(tlvTypeEnd, end)
		if err != nil {
			return nil, err
		}
	}
	if hdr.Compression != "" {
		compression, err := encodeString("s2")
		if err != nil {
			return nil, err
		}
		err = tw.Write(tlvTypeCompression, compression)
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
