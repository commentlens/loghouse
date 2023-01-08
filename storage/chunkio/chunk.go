package chunkio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/commentlens/loghouse/storage"
	"github.com/commentlens/loghouse/storage/tlv"
	"github.com/klauspost/compress/s2"
)

const (
	tlvChunkContainer = iota + 1
	tlvChunkHeader
	tlvChunkData
	tlvLabels
	tlvStart
	tlvEnd
	tlvCompression
	tlvTime
	tlvString
)

func Read(ctx context.Context, r io.Reader, opts *storage.ReadOptions) error {
	tr := tlv.NewReader(r)
	for {
		typChunk, valChunk, err := tr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if typChunk != tlvChunkContainer {
			continue
		}
		err = readChunk(ctx, valChunk, opts)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func readChunk(ctx context.Context, val io.Reader, opts *storage.ReadOptions) error {
	tr := tlv.NewReader(val)
	typHeader, valHeader, err := tr.Read()
	if err != nil {
		return err
	}
	if typHeader != tlvChunkHeader {
		return nil
	}
	hdr, err := decodeHeader(valHeader)
	if err != nil {
		return err
	}
	if !storage.MatchLabels(hdr.Labels, opts.Labels) {
		return nil
	}
	if !opts.Start.IsZero() && opts.Start.After(hdr.End) {
		return nil
	}
	if !opts.End.IsZero() && opts.End.Before(hdr.Start) {
		return nil
	}
	typData, valData, err := tr.Read()
	if err != nil {
		return err
	}
	if typData != tlvChunkData {
		return nil
	}
	return readData(ctx, hdr, valData, opts)
}

type chunkHeader struct {
	Labels      map[string]string
	Start       time.Time
	End         time.Time
	Compression string
}

func decodeHeader(val io.Reader) (*chunkHeader, error) {
	var hdr chunkHeader
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
		case tlvLabels:
			m, err := decodeMap(val)
			if err != nil {
				return nil, err
			}
			hdr.Labels = m
		case tlvStart:
			t, err := decodeTime(val)
			if err != nil {
				return nil, err
			}
			hdr.Start = t
		case tlvEnd:
			t, err := decodeTime(val)
			if err != nil {
				return nil, err
			}
			hdr.End = t
		case tlvCompression:
			s, err := decodeString(val)
			if err != nil {
				return nil, err
			}
			hdr.Compression = s
		}
	}
	return &hdr, nil
}

func readData(ctx context.Context, hdr *chunkHeader, val io.Reader, opts *storage.ReadOptions) error {
	switch hdr.Compression {
	case "s2":
		val = s2.NewReader(val)
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
		if typTime != tlvTime {
			return nil
		}
		t, err := decodeTime(valTime)
		if err != nil {
			return err
		}
		typStr, valStr, err := tr.Read()
		if err != nil {
			return err
		}
		if typStr != tlvString {
			return nil
		}
		s, err := decodeString(valStr)
		if err != nil {
			return err
		}
		e := storage.LogEntry{
			Labels: hdr.Labels,
			Time:   t,
			Data:   storage.LogEntryData(s),
		}
		if opts.FilterFunc != nil {
			if !opts.FilterFunc(&e) {
				continue
			}
		}
		opts.ResultFunc(&e)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func Write(w io.Writer, es []*storage.LogEntry) error {
	if len(es) == 0 {
		return nil
	}
	sort.SliceStable(es, func(i, j int) bool { return es[i].Time.Before(es[j].Time) })

	tw := tlv.NewWriter(w)
	chunk, err := encodeChunk(es)
	if err != nil {
		return err
	}
	return tw.Write(tlvChunkContainer, chunk)
}

func encodeChunk(es []*storage.LogEntry) ([]byte, error) {
	buf := new(bytes.Buffer)

	tw := tlv.NewWriter(buf)
	header, err := encodeHeader(es)
	if err != nil {
		return nil, err
	}
	err = tw.Write(tlvChunkHeader, header)
	if err != nil {
		return nil, err
	}
	data, err := encodeData(es)
	if err != nil {
		return nil, err
	}
	err = tw.Write(tlvChunkData, data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeHeader(es []*storage.LogEntry) ([]byte, error) {
	buf := new(bytes.Buffer)

	tw := tlv.NewWriter(buf)
	labels, err := encodeMap(es[0].Labels)
	if err != nil {
		return nil, err
	}
	err = tw.Write(tlvLabels, labels)
	if err != nil {
		return nil, err
	}
	start, err := encodeTime(es[0].Time)
	if err != nil {
		return nil, err
	}
	err = tw.Write(tlvStart, start)
	if err != nil {
		return nil, err
	}
	end, err := encodeTime(es[len(es)-1].Time)
	if err != nil {
		return nil, err
	}
	err = tw.Write(tlvEnd, end)
	if err != nil {
		return nil, err
	}
	compression, err := encodeString("s2")
	if err != nil {
		return nil, err
	}
	err = tw.Write(tlvCompression, compression)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeData(es []*storage.LogEntry) ([]byte, error) {
	buf := new(bytes.Buffer)
	compress := s2.NewWriter(buf)
	tw := tlv.NewWriter(compress)
	for _, e := range es {
		t, err := encodeTime(e.Time)
		if err != nil {
			return nil, err
		}
		err = tw.Write(tlvTime, t)
		if err != nil {
			return nil, err
		}
		d, err := encodeString(string(e.Data))
		if err != nil {
			return nil, err
		}
		err = tw.Write(tlvString, d)
		if err != nil {
			return nil, err
		}
	}
	err := compress.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeString(s string) ([]byte, error) {
	return []byte(s), nil
}

func decodeString(val io.Reader) (string, error) {
	b, err := io.ReadAll(val)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func encodeTime(t time.Time) ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(t.UnixMilli()))
	return b, nil
}

func decodeTime(val io.Reader) (time.Time, error) {
	b, err := io.ReadAll(val)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(int64(binary.BigEndian.Uint64(b))).UTC(), nil
}

func encodeMap(m map[string]string) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := tlv.NewWriter(buf)
	for k, v := range m {
		for _, s := range []string{k, v} {
			b, err := encodeString(s)
			if err != nil {
				return nil, err
			}
			err = w.Write(tlvString, b)
			if err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func decodeMap(val io.Reader) (map[string]string, error) {
	r := tlv.NewReader(val)
	var l []string
	for {
		typ, val, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		switch typ {
		case tlvString:
			s, err := decodeString(val)
			if err != nil {
				return nil, err
			}
			l = append(l, s)
		}
	}
	m := make(map[string]string)
	for i := 0; i+1 < len(l); i += 2 {
		m[l[i]] = l[i+1]
	}
	return m, nil
}
