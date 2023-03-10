package loki

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"regexp/syntax"
	"sort"
	"strconv"
	"strings"

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
	var filters []func(e storage.LogEntry) bool
	var contains []string
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
				filters = append(filters, func(e storage.LogEntry) bool {
					v, ok := e.Labels[key]
					if !ok {
						return false
					}
					return v != val
				})
			case "=~":
				re, err := regexp.Compile(val)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					v, ok := e.Labels[key]
					if !ok {
						return false
					}
					return re.MatchString(v)
				})
			case "!~":
				re, err := regexp.Compile(val)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					v, ok := e.Labels[key]
					if !ok {
						return false
					}
					return !re.MatchString(v)
				})
			}
		case symbols.NT_LineFilter:
			op := node.GetNTChildI(0).GetTChildI(0).LiteralString()
			val := node.GetTChildI(1).LiteralString()
			val, err := logqlUnquote(val)
			if err != nil {
				return err
			}
			if val == "" {
				return nil
			}
			parseRequired := strings.ContainsAny(val, `:,{}[]"`) || val != logqlQuote(val)
			switch op {
			case "|=":
				bVal := []byte(val)
				filters = append(filters, func(e storage.LogEntry) bool {
					if !parseRequired {
						return bytes.Contains(e.Data, bVal)
					}
					ts, err := e.Data.Values()
					if err != nil {
						return false
					}
					for _, t := range ts {
						if strings.Contains(t, val) {
							return true
						}
					}
					return false
				})
				contains = append(contains, val)
			case "!=":
				// negate
				bVal := []byte(val)
				filters = append(filters, func(e storage.LogEntry) bool {
					if !parseRequired {
						return !bytes.Contains(e.Data, bVal)
					}
					ts, err := e.Data.Values()
					if err != nil {
						return false
					}
					for _, t := range ts {
						if strings.Contains(t, val) {
							return false
						}
					}
					return true
				})
			case "|~":
				re, err := regexp.Compile(val)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					if !parseRequired {
						return re.Match(e.Data)
					}
					ts, err := e.Data.Values()
					if err != nil {
						return false
					}
					for _, t := range ts {
						if re.MatchString(t) {
							return true
						}
					}
					return false
				})
				litVals, err := regexpExtractLiterals(val)
				if err != nil {
					return err
				}
				contains = append(contains, litVals...)
			case "!~":
				// negate
				re, err := regexp.Compile(val)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					if !parseRequired {
						return !re.Match(e.Data)
					}
					ts, err := e.Data.Values()
					if err != nil {
						return false
					}
					for _, t := range ts {
						if re.MatchString(t) {
							return false
						}
					}
					return true
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
			litKeys, err := gjsonExtractLiterals(key)
			if err != nil {
				return err
			}
			contains = append(contains, litKeys...)
			switch op {
			case "=":
				filters = append(filters, func(e storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return v.String() == val
				})
				contains = append(contains, val)
			case "!=":
				// negate
				filters = append(filters, func(e storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return v.String() != val
				})
			case "=~":
				re, err := regexp.Compile(val)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return re.MatchString(v.String())
				})
				litVals, err := regexpExtractLiterals(val)
				if err != nil {
					return err
				}
				contains = append(contains, litVals...)
			case "!~":
				// negate
				re, err := regexp.Compile(val)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return !re.MatchString(v.String())
				})
			case ">=":
				fVal, err := strconv.ParseFloat(val, 64)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return v.Float() >= fVal
				})
			case ">":
				fVal, err := strconv.ParseFloat(val, 64)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return v.Float() > fVal
				})
			case "<=":
				fVal, err := strconv.ParseFloat(val, 64)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return v.Float() <= fVal
				})
			case "<":
				fVal, err := strconv.ParseFloat(val, 64)
				if err != nil {
					return err
				}
				filters = append(filters, func(e storage.LogEntry) bool {
					v := gjson.GetBytes(e.Data, key)
					if !v.Exists() {
						return false
					}
					return v.Float() < fVal
				})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(filters) > 0 {
		ropts.SummaryFunc = nil
		ropts.Contains = contains
	}
	if ropts.FilterFunc != nil {
		filters = append(filters, ropts.FilterFunc)
	}
	ropts.FilterFunc = func(e storage.LogEntry) bool {
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

func logqlQuote(s string) string {
	raw := strconv.Quote(s)
	return raw[1 : len(raw)-1]
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

func gjsonExtractLiterals(s string) ([]string, error) {
	root, err := syntax.Parse(s, syntax.POSIX)
	if err != nil {
		return nil, err
	}
	m := make(map[string]struct{})
	isNumber := func(s string) bool {
		for _, r := range s {
			if !('0' <= r && r <= '9') {
				return false
			}
		}
		return true
	}
	f := func(re *syntax.Regexp) bool {
		switch re.Op {
		case syntax.OpStar, syntax.OpQuest:
			return false
		case syntax.OpRepeat:
			if re.Min == 0 {
				return false
			}
		case syntax.OpLiteral:
		default:
			return true
		}
		part := string(re.Rune)
		if part == "" {
			return true
		}
		if part == "#" {
			return true
		}
		if isNumber(part) {
			return true
		}
		m[part] = struct{}{}
		return true
	}
	var walk func(re *syntax.Regexp)
	walk = func(re *syntax.Regexp) {
		ok := f(re)
		if !ok {
			return
		}
		for _, sub := range re.Sub {
			walk(sub)
		}
	}
	walk(root)
	var lits []string
	for k := range m {
		lits = append(lits, k)
	}
	sort.Strings(lits)
	return lits, nil
}

func regexpExtractLiterals(s string) ([]string, error) {
	root, err := syntax.Parse(s, syntax.Perl)
	if err != nil {
		return nil, err
	}
	m := make(map[string]struct{})
	f := func(re *syntax.Regexp) bool {
		switch re.Op {
		case syntax.OpStar, syntax.OpQuest:
			return false
		case syntax.OpRepeat:
			if re.Min == 0 {
				return false
			}
		case syntax.OpLiteral:
		default:
			return true
		}
		part := string(re.Rune)
		if part == "" {
			return true
		}
		m[part] = struct{}{}
		return true
	}
	var walk func(re *syntax.Regexp)
	walk = func(re *syntax.Regexp) {
		ok := f(re)
		if !ok {
			return
		}
		for _, sub := range re.Sub {
			walk(sub)
		}
	}
	walk(root)
	var lits []string
	for k := range m {
		lits = append(lits, k)
	}
	sort.Strings(lits)
	return lits, nil
}
