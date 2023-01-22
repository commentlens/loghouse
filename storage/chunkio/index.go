package chunkio

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/fnv"
	"io"
	"runtime"
	"sync"

	"github.com/FastFilter/xorfilter"
	"github.com/commentlens/loghouse/storage/tlv"
	"golang.org/x/sync/errgroup"
)

type Index struct {
	L1 *xorfilter.BinaryFuse8
	L3 *xorfilter.BinaryFuse8
	L5 *xorfilter.BinaryFuse8
}

func (index *Index) Build(data [][]byte) error {
	g, _ := errgroup.WithContext(context.Background())
	for _, tv := range []struct {
		length int
		filter **xorfilter.BinaryFuse8
	}{
		{
			length: 5,
			filter: &index.L5,
		},
		{
			length: 3,
			filter: &index.L3,
		},
		{
			length: 1,
			filter: &index.L1,
		},
	} {
		tv := tv
		g.Go(func() error {
			return index.build(tv.length, tv.filter, data)
		})
	}
	return g.Wait()
}

func (index *Index) build(length int, f **xorfilter.BinaryFuse8, data [][]byte) error {
	chIn := make(chan [][]byte)
	chOut := make(chan map[uint64]struct{})
	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for data := range chIn {
				m := make(map[uint64]struct{})
				for _, b := range data {
					rs := []rune(string(b))
					for i := 0; i < len(rs)-length+1; i++ {
						h := fnv.New64a()
						h.Write([]byte(string(rs[i : i+length])))
						m[h.Sum64()] = struct{}{}
					}
				}
				chOut <- m
			}
		}()
	}
	go func() {
		batchSize := 1000
		for i := 0; i < len(data); i += batchSize {
			j := i + batchSize
			if j > len(data) {
				j = len(data)
			}
			chIn <- data[i:j]
		}
		close(chIn)
		wg.Wait()
		close(chOut)
	}()
	m := make(map[uint64]struct{})
	for m2 := range chOut {
		for key := range m2 {
			m[key] = struct{}{}
		}
	}
	var keys []uint64
	for k := range m {
		keys = append(keys, k)
	}
	nf, err := xorfilter.PopulateBinaryFuse8(keys)
	if err != nil {
		return err
	}
	*f = nf
	return nil
}

func (index *Index) Contains(b []byte) bool {
	rs := []rune(string(b))
	for _, tv := range []struct {
		length int
		filter *xorfilter.BinaryFuse8
	}{
		{
			length: 5,
			filter: index.L5,
		},
		{
			length: 3,
			filter: index.L3,
		},
		{
			length: 1,
			filter: index.L1,
		},
	} {
		if len(rs) >= tv.length {
			for i := 0; i < len(rs)-tv.length+1; i++ {
				h := fnv.New64a()
				h.Write([]byte(string(rs[i : i+tv.length])))
				if !tv.filter.Contains(h.Sum64()) {
					return false
				}
			}
			return true
		}
	}
	return false
}

func WriteIndex(w io.Writer, index *Index) error {
	b, err := encodeIndex(index)
	if err != nil {
		return err
	}
	tw := tlv.NewWriter(w)
	return tw.Write(tlvTypeIndex, b)
}

func ReadIndex(r io.Reader) (*Index, error) {
	tr := tlv.NewReader(r)
	typ, val, err := tr.Read()
	if err != nil {
		return nil, err
	}
	if typ != tlvTypeIndex {
		return nil, ErrUnexpectedTLVType
	}
	return decodeIndex(val)
}

func encodeIndex(index *Index) ([]byte, error) {
	buf := new(bytes.Buffer)

	tw := tlv.NewWriter(buf)
	for _, tv := range []struct {
		length uint64
		filter *xorfilter.BinaryFuse8
	}{
		{
			length: 1,
			filter: index.L1,
		},
		{
			length: 3,
			filter: index.L3,
		},
		{
			length: 5,
			filter: index.L5,
		},
	} {
		b, err := encodeFilter(tv.filter)
		if err != nil {
			return nil, err
		}
		err = tw.Write(tv.length, b)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeIndex(val io.Reader) (*Index, error) {
	var index Index
	tr := tlv.NewReader(val)
	for _, tv := range []struct {
		length uint64
		filter **xorfilter.BinaryFuse8
	}{
		{
			length: 1,
			filter: &index.L1,
		},
		{
			length: 3,
			filter: &index.L3,
		},
		{
			length: 5,
			filter: &index.L5,
		},
	} {
		typ, val, err := tr.Read()
		if err != nil {
			return nil, err
		}
		if typ != tv.length {
			return nil, ErrUnexpectedTLVType
		}
		b, err := val.ReadAll()
		if err != nil {
			return nil, err
		}
		f, err := decodeFilter(b)
		if err != nil {
			return nil, err
		}
		*tv.filter = f
	}
	return &index, nil
}

func encodeFilter(f *xorfilter.BinaryFuse8) ([]byte, error) {
	buf := new(bytes.Buffer)

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], f.Seed)
	_, err := buf.Write(b[:])
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(b[:4], f.SegmentLength)
	_, err = buf.Write(b[:4])
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(b[:4], f.SegmentLengthMask)
	_, err = buf.Write(b[:4])
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(b[:4], f.SegmentCount)
	_, err = buf.Write(b[:4])
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(b[:4], f.SegmentCountLength)
	_, err = buf.Write(b[:4])
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(f.Fingerprints)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeFilter(b []byte) (*xorfilter.BinaryFuse8, error) {
	var f xorfilter.BinaryFuse8
	f.Seed = binary.BigEndian.Uint64(b[0:8])
	f.SegmentLength = binary.BigEndian.Uint32(b[8:12])
	f.SegmentLengthMask = binary.BigEndian.Uint32(b[12:16])
	f.SegmentCount = binary.BigEndian.Uint32(b[16:20])
	f.SegmentCountLength = binary.BigEndian.Uint32(b[20:24])
	f.Fingerprints = b[24:]
	return &f, nil
}
