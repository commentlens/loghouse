package loki

import (
	"github.com/commentlens/loghouse/api/loki/logql/parser/bsr"
	"github.com/commentlens/loghouse/api/loki/logql/parser/symbols"
	"github.com/commentlens/loghouse/storage"
	"github.com/davecgh/go-spew/spew"
)

func logqlRead(r storage.Reader, optsf func() *storage.ReadOptions, root bsr.BSR) ([]*storage.LogEntry, bool, error) {
	var isHistogram bool
	filter := func(e *storage.LogEntry) bool {
		return true
	}
	ropts := optsf()
	logqlWalk(root, func(node bsr.BSR) {
		switch node.Label.Slot().NT {
		case symbols.NT_MetricQuery:
			isHistogram = true
		case symbols.NT_LogSelectorMember:
			spew.Dump(node.Label)
		case symbols.NT_LineFilter:
			spew.Dump(node.Label)
		case symbols.NT_LabelFilter:
			spew.Dump(node.Label)
		}
	})
	es, err := r.Read(ropts)
	if err != nil {
		return nil, false, err
	}
	var out []*storage.LogEntry
	for _, e := range es {
		if filter(e) {
			out = append(out, e)
		}
	}
	return out, isHistogram, nil
}

func logqlWalk(node bsr.BSR, f func(bsr.BSR)) {
	f(node)
	for _, nts := range node.GetAllNTChildren() {
		for _, nt := range nts {
			logqlWalk(nt, f)
		}
	}
}
