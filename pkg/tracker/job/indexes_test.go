package job

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func int32Ptr(v int32) *int32 { return &v }

func TestParseIndexSet_ValidInputs(t *testing.T) {
	defaultCompletions := int32Ptr(10)

	for _, tc := range []struct {
		name           string
		raw            string
		nilCompletions bool
		completions    *int32
		wantRanges     []IndexRange
		wantCount      int32
		wantString     string
	}{
		{
			name:       "empty string is an empty set",
			raw:        "",
			wantRanges: nil,
			wantCount:  0,
			wantString: "",
		},
		{
			name:       "single index",
			raw:        "3",
			wantRanges: []IndexRange{{3, 3}},
			wantCount:  1,
			wantString: "3",
		},
		{
			name:       "continuous range",
			raw:        "3-7",
			wantRanges: []IndexRange{{3, 7}},
			wantCount:  5,
			wantString: "3-7",
		},
		{
			name:       "multiple ranges",
			raw:        "1,3-5,7",
			wantRanges: []IndexRange{{1, 1}, {3, 5}, {7, 7}},
			wantCount:  5,
			wantString: "1,3-5,7",
		},
		{
			name:       "unordered elements are sorted and normalized",
			raw:        "7,1,4,3-5",
			wantRanges: []IndexRange{{1, 1}, {3, 5}, {7, 7}},
			wantCount:  5,
			wantString: "1,3-5,7",
		},
		{
			name:       "duplicated indexes are counted once",
			raw:        "2,2,2",
			wantRanges: []IndexRange{{2, 2}},
			wantCount:  1,
			wantString: "2",
		},
		{
			name:       "overlapping ranges are merged",
			raw:        "1-4,3-6",
			wantRanges: []IndexRange{{1, 6}},
			wantCount:  6,
			wantString: "1-6",
		},
		{
			name:       "adjacent ranges are merged into one series",
			raw:        "1-3,4-6",
			wantRanges: []IndexRange{{1, 6}},
			wantCount:  6,
			wantString: "1-6",
		},
		{
			name:       "two consecutive indexes render as two tokens, count stays two",
			raw:        "4-5",
			wantRanges: []IndexRange{{4, 5}},
			wantCount:  2,
			wantString: "4,5",
		},
		{
			name:       "whitespace around tokens is tolerated",
			raw:        " 1 , 3 - 5 ",
			wantRanges: []IndexRange{{1, 1}, {3, 5}},
			wantCount:  4,
			wantString: "1,3-5",
		},
		{
			name:           "huge range never expands, count stays stable",
			raw:            "0-1000000",
			nilCompletions: true,
			wantRanges:     []IndexRange{{0, 1000000}},
			wantCount:      1000001,
			wantString:     "0-1000000",
		},
		{
			name:       "boundary indexes of completions domain are accepted",
			raw:        "0,9",
			wantRanges: []IndexRange{{0, 0}, {9, 9}},
			wantCount:  2,
			wantString: "0,9",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var completions *int32
			if !tc.nilCompletions {
				completions = defaultCompletions
			}
			set, err := ParseIndexSet("completedIndexes", tc.raw, completions)
			require.NoError(t, err)
			assert.Equal(t, tc.wantRanges, set.Ranges())

			count, err := set.Count()
			require.NoError(t, err)
			assert.Equal(t, tc.wantCount, count)
			assert.Equal(t, tc.wantString, set.String())
		})
	}
}

func TestParseIndexSet_InvalidInputs(t *testing.T) {
	completions := int32Ptr(5)

	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"negative single index", "-1"},
		{"negative range bound", "-2-3"},
		{"inverted range", "5-2"},
		{"non-numeric character", "a"},
		{"non-numeric inside range", "1-x"},
		{"plus sign is not plain digits", "+1"},
		{"float value", "1.5"},
		{"empty element in the middle", "1,,3"},
		{"trailing comma", "1,2,"},
		{"leading comma", ",1,2"},
		{"malformed interval with two hyphens", "1-2-3"},
		{"empty range bound", "1-"},
		{"whitespace-only token", "1, ,2"},
		{"index equals completions is outside domain", "5"},
		{"range end outside completions domain", "3-6"},
		{"range start outside completions domain", "9-10"},
		{"int32 overflow", "2147483648"},
		{"int64 overflow", "99999999999999999999"},
		{"overflow inside range", "1-99999999999999999999"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseIndexSet("completedIndexes", tc.raw, completions)
			require.Error(t, err)

			var parseErr *IntervalParseError
			require.ErrorAs(t, err, &parseErr)
			assert.Equal(t, "completedIndexes", parseErr.Field)
			assert.Equal(t, tc.raw, parseErr.Raw)
			assert.NotEmpty(t, parseErr.Msg)
		})
	}
}

func TestParseIndexSet_NilCompletionsSkipsDomainCheck(t *testing.T) {
	// When spec.completions is unknown (work-queue style or older data) the
	// grammar is still validated but no domain bound can be enforced.
	set, err := ParseIndexSet("failedIndexes", "100-200", nil)
	require.NoError(t, err)
	count, err := set.Count()
	require.NoError(t, err)
	assert.Equal(t, int32(101), count)
}

func TestIndexSet_HasAndIntersects(t *testing.T) {
	set, err := ParseIndexSet("completedIndexes", "1,3-5,7", int32Ptr(10))
	require.NoError(t, err)

	for _, idx := range []int32{1, 3, 4, 5, 7} {
		assert.True(t, set.Has(idx), "expected set to contain %d", idx)
	}
	for _, idx := range []int32{0, 2, 6, 8, 9} {
		assert.False(t, set.Has(idx), "expected set not to contain %d", idx)
	}

	overlapping, err := ParseIndexSet("failedIndexes", "5-6", int32Ptr(10))
	require.NoError(t, err)
	assert.True(t, set.Intersects(overlapping))

	disjoint, err := ParseIndexSet("failedIndexes", "0,2", int32Ptr(10))
	require.NoError(t, err)
	assert.False(t, set.Intersects(disjoint))

	empty, err := ParseIndexSet("failedIndexes", "", int32Ptr(10))
	require.NoError(t, err)
	assert.False(t, set.Intersects(empty))
}

func TestParseIndexSet_NormalizationIsOrderIndependent(t *testing.T) {
	completions := int32Ptr(20)

	variants := []string{
		"1,3-5,7,10-12",
		"10-12,7,3-5,1",
		"1,7,10-12,5,4,3",
		"1,3,4,5,7,10,11,12",
		"3-5,10-12,1,7",
	}

	var reference string
	var refCount int32
	for i, raw := range variants {
		set, err := ParseIndexSet("completedIndexes", raw, completions)
		require.NoError(t, err)
		rendered := set.String()
		count, err := set.Count()
		require.NoError(t, err)

		if i == 0 {
			reference = rendered
			refCount = count
		} else {
			assert.Equal(t, reference, rendered, "variant %q normalized differently", raw)
			assert.Equal(t, refCount, count)
		}
	}
	assert.Equal(t, "1,3-5,7,10-12", reference)
	assert.Equal(t, int32(8), refCount)
}

func TestIndexSet_FormatBounded(t *testing.T) {
	// 20 non-adjacent single indexes -> 20 distinct ranges.
	var raw string
	for i := 0; i < 20; i++ {
		if i > 0 {
			raw += ","
		}
		raw += strconv.Itoa(i * 2)
	}
	set, err := ParseIndexSet("completedIndexes", raw, int32Ptr(40))
	require.NoError(t, err)
	require.Len(t, set.Ranges(), 20)

	t.Run("range cap keeps leading tokens and reports omitted count", func(t *testing.T) {
		out := set.FormatBounded(3, 0)
		assert.Equal(t, "0,2,4,...(+17 more indexes)", out)
	})

	t.Run("no cap renders everything", func(t *testing.T) {
		out := set.FormatBounded(0, 0)
		assert.Equal(t, raw, out)
	})

	t.Run("char cap keeps only whole tokens plus the omission suffix", func(t *testing.T) {
		out := set.FormatBounded(0, 10)
		assert.Contains(t, out, "more indexes")
		assert.True(t, len(out) <= 10+len(",...(+20 more indexes)"))
		// Everything before the suffix must remain a valid prefix of whole tokens.
		prefix := out[:strings.Index(out, ",...")]
		_, err := ParseIndexSet("completedIndexes", prefix, nil)
		require.NoError(t, err)
	})

	t.Run("huge range stays a single bounded token", func(t *testing.T) {
		big, err := ParseIndexSet("completedIndexes", "0-1000000", nil)
		require.NoError(t, err)
		out := big.FormatBounded(8, 120)
		assert.Equal(t, "0-1000000", out)
	})

	t.Run("empty set renders empty", func(t *testing.T) {
		empty, err := ParseIndexSet("completedIndexes", "", int32Ptr(5))
		require.NoError(t, err)
		assert.Empty(t, empty.FormatBounded(8, 120))
	})
}

func TestParseIndexSet_MaxInt32Boundary(t *testing.T) {
	// math.MaxInt32 parses fine without a domain bound; one more cannot.
	set, err := ParseIndexSet("completedIndexes", "2147483647", nil)
	require.NoError(t, err)
	count, err := set.Count()
	require.NoError(t, err)
	assert.Equal(t, int32(1), count)
	assert.True(t, set.Has(math.MaxInt32))

	_, err = ParseIndexSet("completedIndexes", "2147483648", nil)
	require.Error(t, err)
}
