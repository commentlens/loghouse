package loki

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/commentlens/loghouse/api/loki/logql/lexer"
	"github.com/commentlens/loghouse/api/loki/logql/parser"
	"github.com/commentlens/loghouse/api/loki/logql/parser/bsr"
	"github.com/commentlens/loghouse/api/loki/logql/parser/symbols"
	"github.com/commentlens/loghouse/storage"
	"github.com/tidwall/gjson"
)

func logqlRead(ctx context.Context, r storage.Reader, ropts *storage.ReadOptions, query string) error {
	root, err := logqlParse(query)
	if err != nil {
		return err
	}
	var filters []func(e *storage.LogEntry) bool
	err = logqlWalk(root, func(node bsr.BSR) error {
		switch node.Label.Slot().NT {
		case symbols.NT_LogSelectorMember:
			key := node.GetNTChildI(0).GetTChildI(0).LiteralString()
			op := node.GetNTChildI(1).GetTChildI(0).LiteralString()
			val := node.GetTChildI(2).LiteralString()
			val, err := logqlUnquote(val)
			if err != nil {
				return err
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
			val, err := logqlUnquote(val)
			if err != nil {
				return err
			}
			bVal := []byte(val)
			switch op {
			case "|=":
				filters = append(filters, func(e *storage.LogEntry) bool {
					return bytes.Contains(e.Data, bVal)
				})
			case "!=":
				filters = append(filters, func(e *storage.LogEntry) bool {
					return !bytes.Contains(e.Data, bVal)
				})
			case "|~":
				filters = append(filters, func(e *storage.LogEntry) bool {
					ok, err := regexp.Match(val, e.Data)
					if err != nil {
						return false
					}
					return ok
				})
			case "!~":
				filters = append(filters, func(e *storage.LogEntry) bool {
					ok, err := regexp.Match(val, e.Data)
					if err != nil {
						return false
					}
					return !ok
				})
			}
		case symbols.NT_DataFilter:
			key := node.GetTChildI(1).LiteralString()
			key, err := logqlUnquote(key)
			if err != nil {
				return err
			}
			op := node.GetNTChildI(2).GetTChildI(0).LiteralString()
			val := node.GetTChildI(3).LiteralString()
			val, err = logqlUnquote(val)
			if err != nil {
				return err
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
		return nil
	})
	if err != nil {
		return err
	}
	ropts.FilterFunc = func(e *storage.LogEntry) bool {
		for _, filter := range filters {
			if !filter(e) {
				return false
			}
		}
		return true
	}
	return r.Read(ctx, ropts)
}

func logqlParse(query string) (bsr.BSR, error) {
	lex := lexer.New([]rune(query))
	q, errs := parser.Parse(lex)
	if len(errs) > 0 {
		return bsr.BSR{}, fmt.Errorf("logql: %s", errs[0].String())
	}
	if q.IsAmbiguous() {
		return bsr.BSR{}, fmt.Errorf("logql: ambiguous query %q", query)
	}
	return q.GetRoot(), nil
}

func logqlUnquote(s string) (string, error) {
	raw, err := strconv.Unquote(s)
	if err != nil {
		return "", fmt.Errorf("logql: %w %s", err, s)
	}
	return raw, nil
}

func logqlWalk(node bsr.BSR, f func(bsr.BSR) error) error {
	err := f(node)
	if err != nil {
		return err
	}
	for _, nts := range node.GetAllNTChildren() {
		for _, nt := range nts {
			err := logqlWalk(nt, f)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func logqlIsHistogram(query string) (bool, error) {
	root, err := logqlParse(query)
	if err != nil {
		return false, err
	}
	var isHistogram bool
	logqlWalk(root, func(node bsr.BSR) error {
		switch node.Label.Slot().NT {
		case symbols.NT_MetricQuery:
			isHistogram = true
		}
		return nil
	})
	return isHistogram, nil
}
