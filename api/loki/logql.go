package loki

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/commentlens/loghouse/api/loki/logql/lexer"
	"github.com/commentlens/loghouse/api/loki/logql/parser"
	"github.com/commentlens/loghouse/api/loki/logql/parser/bsr"
	"github.com/commentlens/loghouse/api/loki/logql/parser/symbols"
	"github.com/commentlens/loghouse/storage"
	"github.com/davecgh/go-spew/spew"
	"github.com/tidwall/gjson"
)

func logqlRead(r storage.Reader, f func() *storage.ReadOptions, query string) ([]*storage.LogEntry, bool, error) {
	lex := lexer.New([]rune(query))
	q, errs := parser.Parse(lex)
	if len(errs) > 0 {
		spew.Dump(errs)
		return nil, false, fmt.Errorf("logql: parse query %q", query)
	}
	if q.IsAmbiguous() {
		q.ReportAmbiguous()
		return nil, false, fmt.Errorf("logql: ambiguous query %q", query)
	}
	root := q.GetRoot()

	var isHistogram bool
	var filters []func(e *storage.LogEntry) bool
	ropts := f()
	logqlWalk(root, func(node bsr.BSR) {
		switch node.Label.Slot().NT {
		case symbols.NT_MetricQuery:
			isHistogram = true
			ropts.Limit = 0
		case symbols.NT_LogSelectorMember:
			key := node.GetNTChildI(0).GetTChildI(0).LiteralString()
			op := node.GetNTChildI(1).GetTChildI(0).LiteralString()
			val := node.GetTChildI(2).LiteralString()
			val, err := strconv.Unquote(val)
			if err != nil {
				return
			}
			switch op {
			case "=":
				if ropts.Labels == nil {
					ropts.Labels = make(map[string]string)
				}
				ropts.Labels[key] = val
			case "!=":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v, ok := e.Labels[key]
					if !ok {
						return false
					}
					return v != val
				})
			case "=~":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v, ok := e.Labels[key]
					if !ok {
						return false
					}
					ok, err := regexp.MatchString(val, v)
					if err != nil {
						return false
					}
					return ok
				})
			case "!~":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v, ok := e.Labels[key]
					if !ok {
						return false
					}
					ok, err := regexp.MatchString(val, v)
					if err != nil {
						return false
					}
					return !ok
				})
			}
		case symbols.NT_LineFilter:
			op := node.GetNTChildI(0).GetTChildI(0).LiteralString()
			val := node.GetTChildI(1).LiteralString()
			val, err := strconv.Unquote(val)
			if err != nil {
				return
			}
			switch op {
			case "|=":
				filters = append(filters, func(e *storage.LogEntry) bool {
					return strings.Contains(string(e.Data), val)
				})
			case "!=":
				filters = append(filters, func(e *storage.LogEntry) bool {
					return !strings.Contains(string(e.Data), val)
				})
			case "|~":
				filters = append(filters, func(e *storage.LogEntry) bool {
					ok, err := regexp.MatchString(val, string(e.Data))
					if err != nil {
						return false
					}
					return ok
				})
			case "!~":
				filters = append(filters, func(e *storage.LogEntry) bool {
					ok, err := regexp.MatchString(val, string(e.Data))
					if err != nil {
						return false
					}
					return !ok
				})
			}
		case symbols.NT_LabelFilter:
			var keyParts []string
			for i, sym := range node.GetNTChildI(1).Label.Symbols() {
				if sym.IsNonTerminal() {
					continue
				}
				keyParts = append(keyParts, node.GetNTChildI(1).GetTChildI(i).LiteralString())
			}
			key := strings.Join(keyParts, "")
			op := node.GetNTChildI(2).GetTChildI(0).LiteralString()
			val := node.GetNTChildI(3).GetTChildI(0).LiteralString()
			if strings.ContainsAny(val, "`\"") {
				var err error
				val, err = strconv.Unquote(val)
				if err != nil {
					return
				}
			}
			switch op {
			case "=":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return v.String() == val
				})
			case "!=":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return v.String() != val
				})
			case "=~":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					ok, err := regexp.MatchString(val, v.String())
					if err != nil {
						return false
					}
					return ok
				})
			case "!~":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					ok, err := regexp.MatchString(val, v.String())
					if err != nil {
						return false
					}
					return !ok
				})
			case ">=":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					fval, err := strconv.ParseFloat(val, 64)
					if err != nil {
						return false
					}
					return v.Float() >= fval
				})
			case ">":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					fval, err := strconv.ParseFloat(val, 64)
					if err != nil {
						return false
					}
					return v.Float() > fval
				})
			case "<=":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					fval, err := strconv.ParseFloat(val, 64)
					if err != nil {
						return false
					}
					return v.Float() <= fval
				})
			case "<":
				filters = append(filters, func(e *storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					fval, err := strconv.ParseFloat(val, 64)
					if err != nil {
						return false
					}
					return v.Float() < fval
				})
			}
		}
	})
	ropts.FilterFunc = func(e *storage.LogEntry) bool {
		match := true
		for _, filter := range filters {
			if !filter(e) {
				match = false
				break
			}
		}
		return match
	}
	es, err := r.Read(ropts)
	if err != nil {
		return nil, false, err
	}
	return es, isHistogram, nil
}

func logqlWalk(node bsr.BSR, f func(bsr.BSR)) {
	f(node)
	for _, nts := range node.GetAllNTChildren() {
		for _, nt := range nts {
			logqlWalk(nt, f)
		}
	}
}
