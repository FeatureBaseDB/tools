package dx

import (
	"testing"

	"github.com/pilosa/go-pilosa"
)

func TestIsValidQuery(t *testing.T) {
	res := &pilosa.RowResult{}
	count := int64(50)

	tests := []struct {
		name     string
		query    *Query
		expected bool
	}{
		{
			name:     "nil",
			query:    nil,
			expected: false,
		},
		{
			name:     "empty",
			query:    &Query{},
			expected: false,
		},
		{
			name:     "reult-only",
			query:    &Query{Result: res},
			expected: true,
		},
		{
			name:     "count-only",
			query:    &Query{ResultCount: &count},
			expected: true,
		},
		{
			name:     "result-and-count",
			query:    &Query{Result: res, ResultCount: &count},
			expected: true,
		},
	}

	for _, q := range tests {
		got := isValidQuery(q.query)
		if got != q.expected {
			t.Fatalf("test case %v: expected: %v, got %v", q.name, q.expected, got)
		}
	}
}

func TestQueryResultEqual(t *testing.T) {
	res0 := &pilosa.RowResult{Columns: []uint64{0, 2, 4, 6}}
	res1 := &pilosa.RowResult{Columns: []uint64{0, 2, 4, 6}}
	res2 := &pilosa.RowResult{Columns: []uint64{1, 2, 4, 6}}
	count3 := int64(3)
	count4 := int64(4)
	count4dup := int64(4)

	tests := []struct {
		name     string
		query1   *Query
		query2   *Query
		expected bool
	}{
		{
			name:     "result-result-equal",
			query1:   &Query{Result: res0},
			query2:   &Query{Result: res1},
			expected: true,
		},
		{
			name:     "result-count-equal",
			query1:   &Query{Result: res0},
			query2:   &Query{ResultCount: &count4},
			expected: true,
		},
		{
			name:     "count-count-equal",
			query1:   &Query{ResultCount: &count4},
			query2:   &Query{ResultCount: &count4dup},
			expected: true,
		},
		{
			name:     "result-result-unequal",
			query1:   &Query{Result: res0},
			query2:   &Query{Result: res2},
			expected: false,
		},
		{
			name:     "result-count-unequal",
			query1:   &Query{Result: res0},
			query2:   &Query{ResultCount: &count3},
			expected: false,
		},
		{
			name:     "count-count-unequal",
			query1:   &Query{ResultCount: &count4},
			query2:   &Query{ResultCount: &count3},
			expected: false,
		},
		{
			name:     "default-to-results",
			query1:   &Query{Result: res0, ResultCount: &count4},
			query2:   &Query{Result: res1, ResultCount: &count3},
			expected: true,
		},
	}

	for _, q := range tests {
		got := queryResultsEqual(q.query1, q.query2)
		if got != q.expected {
			t.Fatalf("test case: %v, expected %v, got: %v", q.name, q.expected, got)
		}
	}
}
