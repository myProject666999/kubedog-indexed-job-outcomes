package job

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// IndexRange is a closed interval of completion indexes [Start, End].
// A single index is represented as Start == End.
type IndexRange struct {
	Start int32
	End   int32
}

// IntervalParseError describes why an index interval string
// (Job status .completedIndexes / .failedIndexes format) could not be parsed.
// It always carries the offending raw field name and value so that callers
// can surface an identifiable message instead of silently dropping the data.
type IntervalParseError struct {
	Field string
	Raw   string
	Msg   string
}

func (e *IntervalParseError) Error() string {
	return fmt.Sprintf("invalid %s %q: %s", e.Field, e.Raw, e.Msg)
}

// IndexSet is a normalized, order-independent representation of the index
// sets reported by the Job controller. Internals are sorted merged ranges,
// so huge intervals (e.g. "0-1000000") are never expanded into a slice.
type IndexSet struct {
	ranges []IndexRange
}

// ParseIndexSet parses a Job controller index string, normalizes it and,
// when completions is non-nil, validates every index against the Job's
// spec.completions domain [0, *completions-1].
//
// The accepted grammar is the one documented for batch/v1
// JobStatus.completedIndexes: comma-separated decimal integers or
// hyphen-separated integer pairs, e.g. "1,3-5,7". Inputs may be unordered,
// duplicated or contain adjacent intervals; they are merged deterministically.
//
// Negative values, inverted ranges, non-numeric tokens, integer overflow and
// indexes outside the completions domain produce an *IntervalParseError rather
// than being counted as success.
func ParseIndexSet(field, raw string, completions *int32) (IndexSet, error) {
	ranges, err := parseIndexRanges(field, raw, completions)
	if err != nil {
		return IndexSet{}, err
	}

	return IndexSet{ranges: mergeIndexRanges(ranges)}, nil
}

func parseIndexRanges(field, raw string, completions *int32) ([]IndexRange, error) {
	// An empty string is the controller's representation of an empty set.
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	var ranges []IndexRange
	for _, token := range strings.Split(raw, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			return nil, &IntervalParseError{Field: field, Raw: raw, Msg: "empty interval element"}
		}

		parts := strings.Split(token, "-")
		switch len(parts) {
		case 1:
			value, err := parseIndexToken(field, raw, parts[0])
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, IndexRange{Start: value, End: value})
		case 2:
			start, err := parseIndexToken(field, raw, parts[0])
			if err != nil {
				return nil, err
			}
			end, err := parseIndexToken(field, raw, parts[1])
			if err != nil {
				return nil, err
			}
			if start > end {
				return nil, &IntervalParseError{
					Field: field,
					Raw:   raw,
					Msg:   fmt.Sprintf("inverted interval %d-%d: lower bound must not exceed upper bound", start, end),
				}
			}
			ranges = append(ranges, IndexRange{Start: start, End: end})
		default:
			return nil, &IntervalParseError{Field: field, Raw: raw, Msg: fmt.Sprintf("malformed interval %q: expected N or N-M", token)}
		}
	}

	if completions != nil {
		upper := *completions - 1
		for _, r := range ranges {
			if r.Start < 0 || r.Start > upper || r.End > upper {
				return nil, &IntervalParseError{
					Field: field,
					Raw:   raw,
					Msg:   fmt.Sprintf("index outside spec.completions domain [0,%d]", upper),
				}
			}
		}
	}

	return ranges, nil
}

func parseIndexToken(field, raw, token string) (int32, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, &IntervalParseError{Field: field, Raw: raw, Msg: "empty interval bound"}
	}
	// strconv.ParseInt accepts a leading "+", which is not part of the
	// documented grammar; reject anything that is not plain digits so that
	// values like "+1" produce an identifiable error.
	for _, r := range token {
		if r < '0' || r > '9' {
			return 0, &IntervalParseError{Field: field, Raw: raw, Msg: fmt.Sprintf("non-numeric index %q", token)}
		}
	}

	value, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		// All characters are digits, so the only possible failure here is overflow.
		return 0, &IntervalParseError{Field: field, Raw: raw, Msg: fmt.Sprintf("index %q overflows int32", token)}
	}
	if value > math.MaxInt32 {
		return 0, &IntervalParseError{Field: field, Raw: raw, Msg: fmt.Sprintf("index %d overflows int32", value)}
	}

	return int32(value), nil
}

// mergeIndexRanges sorts, de-duplicates and merges overlapping or adjacent
// ranges. The output is deterministic for any input ordering, which makes the
// normalized count and summary independent of event order.
func mergeIndexRanges(in []IndexRange) []IndexRange {
	if len(in) == 0 {
		return nil
	}

	sorted := make([]IndexRange, len(in))
	copy(sorted, in)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Start < sorted[j].Start
	})

	merged := []IndexRange{sorted[0]}
	for _, r := range sorted[1:] {
		last := &merged[len(merged)-1]
		// Adjacent ranges (last.End+1 == r.Start) describe the same continuous
		// index series and must be represented identically to an explicit range.
		if r.Start <= last.End+1 {
			if r.End > last.End {
				last.End = r.End
			}
			continue
		}
		merged = append(merged, r)
	}

	return merged
}

// Ranges returns the normalized ranges. The returned slice must not be modified.
func (s IndexSet) Ranges() []IndexRange {
	return s.ranges
}

// Count returns the number of distinct indexes. An error is returned only in
// the theoretical case where the represented span does not fit into int32
// (e.g. a range starting at math.MinInt32); controller-reported index sets
// are non-negative and bounded by spec.completions.
func (s IndexSet) Count() (int32, error) {
	var count int64
	for _, r := range s.ranges {
		span := int64(r.End) - int64(r.Start) + 1
		if span <= 0 || count > math.MaxInt32-span {
			return 0, fmt.Errorf("index set count overflows int32")
		}
		count += span
	}

	return int32(count), nil
}

// Has reports whether the set contains the given index.
func (s IndexSet) Has(index int32) bool {
	// Binary search over the sorted merged ranges.
	lo, hi := 0, len(s.ranges)
	for lo < hi {
		mid := (lo + hi) / 2
		switch {
		case index < s.ranges[mid].Start:
			hi = mid
		case index > s.ranges[mid].End:
			lo = mid + 1
		default:
			return true
		}
	}

	return false
}

// Intersects reports whether the two sets share at least one index.
func (s IndexSet) Intersects(other IndexSet) bool {
	i, j := 0, 0
	for i < len(s.ranges) && j < len(other.ranges) {
		a, b := s.ranges[i], other.ranges[j]
		if a.Start <= b.End && b.Start <= a.End {
			return true
		}
		if a.End < b.End {
			i++
		} else {
			j++
		}
	}

	return false
}

// String renders the set in the canonical controller format: a run of one
// index renders as "N", a run of two consecutive indexes as "N,N+1" and only
// runs of three or more consecutive indexes are compressed as "N-M", matching
// the documented JobStatus.completedIndexes representation.
func (s IndexSet) String() string {
	tokens := make([]string, 0, len(s.ranges))
	for _, r := range s.ranges {
		tokens = append(tokens, renderIndexRange(r))
	}

	return strings.Join(tokens, ",")
}

// FormatBounded renders the set like String but never expands an unbounded
// amount of text into logs: at most maxRanges range tokens and maxChars
// characters are emitted, followed by a suffix reporting how many indexes were
// omitted. A non-positive limit disables the corresponding cap.
func (s IndexSet) FormatBounded(maxRanges, maxChars int) string {
	if len(s.ranges) == 0 {
		return ""
	}

	rendered := make([]string, 0, len(s.ranges))
	renderedLen := 0
	shown := int64(0)
	truncated := false

	for i, r := range s.ranges {
		if maxRanges > 0 && i >= maxRanges {
			truncated = true
			break
		}

		token := renderIndexRange(r)
		projected := len(token)
		if len(rendered) > 0 {
			projected++ // separating comma
		}
		if maxChars > 0 && renderedLen+projected > maxChars {
			truncated = true
			break
		}

		rendered = append(rendered, token)
		renderedLen += projected
		shown += int64(r.End) - int64(r.Start) + 1
	}

	out := strings.Join(rendered, ",")
	if truncated {
		total, _ := s.Count()
		out = fmt.Sprintf("%s,...(+%d more indexes)", out, total-int32(shown))
	}

	return out
}

// renderIndexRange renders one normalized range using the controller's
// compression rule: hyphen form only for runs of three or more indexes.
func renderIndexRange(r IndexRange) string {
	switch r.End - r.Start {
	case 0:
		return strconv.FormatInt(int64(r.Start), 10)
	case 1:
		return fmt.Sprintf("%d,%d", r.Start, r.End)
	default:
		return fmt.Sprintf("%d-%d", r.Start, r.End)
	}
}
