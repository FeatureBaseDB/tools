package bench

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pilosa/go-pilosa"
)

var _ Benchmark = (*BasicQueryBenchmark)(nil)

// BasicQueryBenchmark runs a query multiple times with increasing row ids.
type BasicQueryBenchmark struct {
	Name       string `json:"name"`
	MinRowID   int64  `json:"min-row-id"`
	Iterations int    `json:"iterations"`
	NumArgs    int    `json:"num-args"`
	Query      string `json:"query"`
	Index      string `json:"index"`
	Field      string `json:"field"`

	Logger *log.Logger `json:"-"`
}

// NewBasicQueryBenchmark returns a new instance of BasicQueryBenchmark.
func NewBasicQueryBenchmark() *BasicQueryBenchmark {
	return &BasicQueryBenchmark{
		Name:   "basic-query",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the benchmark.
func (b *BasicQueryBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// Initialize schema.
	index, field, err := ensureSchema(client, b.Index, b.Field)
	if err != nil {
		return result, err
	}

	// Determine minimum row id.
	minRowID := b.MinRowID + int64(agentNum*b.Iterations)

	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		rows := make([]*pilosa.PQLRowQuery, b.NumArgs)
		for i := range rows {
			rows[i] = field.Row(minRowID + int64(n))
		}

		var q *pilosa.PQLRowQuery
		switch b.Query {
		case "Intersect":
			q = index.Intersect(rows...)
		case "Union":
			q = index.Union(rows...)
		case "Difference":
			q = index.Difference(rows...)
		case "Xor":
			q = index.Xor(rows...)
		default:
			return result, fmt.Errorf("invalid query type: %q", b.Query)
		}

		start = time.Now()
		_, err := client.Query(q)
		result.Add(time.Since(start), nil)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
