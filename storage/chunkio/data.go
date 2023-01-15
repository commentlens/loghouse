package chunkio

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/tlv"
	"github.com/klauspost/compress/s2"
)

func WriteData(w io.Writer, es []storage.LogEntry, compress bool) error {
	b, err := encodeData(es, compress)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func encodeData(es []storage.LogEntry, compress bool) ([]byte, error) {
	buf := new(bytes.Buffer)
	var w io.Writer = buf
	if compress {
		w = s2.NewWriter(w)
	}
	tw := tlv.NewWriter(w)
	for _, e := range es {
		err := encodeTime(w, tlvTypeStart, e.Time)
		if err != nil {
			return nil, err
		}
		err = tw.Write(tlvTypeString, e.Data)
		if err != nil {
			return nil, err
		}
	}
	if wc, ok := w.(io.WriteCloser); ok {
		err := wc.Close()
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func ReadData(ctx context.Context, hdr *Header, val io.Reader, opts *storage.ReadOptions) error {
	switch hdr.Compression {
	case "s2":
		s2r := newS2Reader()
		defer recycleS2Reader(s2r)
		s2r.Reset(val)
		buf := NewBuffer()
		defer RecycleBuffer(buf)
		buf.Reset(s2r)
		val = buf
	}
	tr := tlv.NewReader(val)
	for {
		typTime, valTime, err := tr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if typTime != tlvTypeStart {
			return ErrUnexpectedTLVType
		}
		t, err := decodeTime(valTime)
		if err != nil {
			return err
		}
		typStr, valStr, err := tr.Read()
		if err != nil {
			return err
		}
		if typStr != tlvTypeString {
			return ErrUnexpectedTLVType
		}
		b, err := readAll(valStr)
		if err != nil {
			return err
		}
		e := storage.LogEntry{
			Labels: hdr.Labels,
			Time:   t,
			Data:   b,
		}
		if !storage.MatchLogEntry(e, opts) {
			continue
		}
		data := make([]byte, len(b))
		copy(data, b)
		e.Data = data
		opts.ResultFunc(e)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}
