package chunkio

import (
	"bytes"
	"encoding/binary"
	"io"
	"runtime"
	"sync"
	"unicode/utf8"

	"github.com/FastFilter/xorfilter"
	"github.com/cespare/xxhash/v2"
	"github.com/commentlens/loghouse/storage/tlv"
)

const (
	indexMaxNgramLength = 8
	indexBuildBatchSize = 1000
)

type Index struct {
	filter *xorfilter.BinaryFuse8
}

func hashRunes(b []byte, length int, m map[uint64]struct{}) {
	offs := make([]int, length)
	var ptr int
	var ok bool
	hash := func(end int) {
		if ptr == 0 && end > 0 {
			ok = true
		}
		start := offs[ptr]
		offs[ptr] = end
		ptr = (ptr + 1) % len(offs)
		if !ok {
			return
		}
		m[xxhash.Sum64(b[start:end])] = struct{}{}
	}
	for i, c := range b {
		if utf8.RuneStart(c) {
			hash(i)
		}
	}
	hash(len(b))
}

func (index *Index) Build(data [][]byte) error {
	workerCount := runtime.NumCPU()
	chIn := make(chan [][]byte, workerCount)
	chOut := make(chan map[uint64]struct{}, workerCount)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for data := range chIn {
				m := make(map[uint64]struct{})
				for _, b := range data {
					for length := 1; length <= indexMaxNgramLength; length++ {
						hashRunes(b, length, m)
					}
				}
				chOut <- m
			}
		}()
	}
	go func() {
		for i := 0; i < len(data); i += indexBuildBatchSize {
			j := i + indexBuildBatchSize
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
	f, err := xorfilter.PopulateBinaryFuse8(keys)
	if err != nil {
		return err
	}
	index.filter = f
	return nil
}

func (index *Index) Contains(s string) bool {
	b := []byte(s)
	length := utf8.RuneCount(b)
	if length > indexMaxNgramLength {
		length = indexMaxNgramLength
	}
	m := make(map[uint64]struct{})
	hashRunes(b, length, m)
	for key := range m {
		if !index.filter.Contains(key) {
			return false
		}
	}
	return true
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
	return encodeFilter(index.filter)
}

func decodeIndex(val io.Reader) (*Index, error) {
	buf := newBuffer()
	defer recycleBuffer(buf)

	_, err := buf.ReadFrom(val)
	if err != nil {
		return nil, err
	}
	f, err := decodeFilter(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return &Index{filter: f}, nil
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
	f.Fingerprints = make([]byte, len(b[24:]))
	copy(f.Fingerprints, b[24:])
	return &f, nil
}
