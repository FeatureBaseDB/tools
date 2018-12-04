package bench

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/pilosa/go-pilosa"
)

// RangeQueryBenchmark runs Range query randomly.
type RangeQueryBenchmark struct {
	Name       string `json:"name"`
	MaxDepth   int    `json:"max-depth"`
	MaxArgs    int    `json:"max-args"`
	MaxN       int    `json:"max-n"`
	MinRange   int64  `json:"min-range"`
	MaxRange   int64  `json:"max-range"`
	Iterations int    `json:"iterations"`
	Seed       int64  `json:"seed"`
	Frame      string `json:"frame"`
	Index      string `json:"index"`
	Field      string `json:"field"`
	QueryType  string `json:"type"`

	Logger *log.Logger `json:"-"`
}

// NewRangeQueryBenchmark returns a new instance of RangeQueryBenchmark.
func NewRangeQueryBenchmark() *RangeQueryBenchmark {
	return &RangeQueryBenchmark{
		Name:   "range-query",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the benchmark.
func (b *RangeQueryBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// Initialize schema.
	index, field, err := ensureSchema(client, b.Index, b.Field)
	if err != nil {
		return result, err
	}

	g := NewQueryGenerator(index, field, b.Seed)
	for n := 0; n < b.Iterations; n++ {
		start := time.Now()
		_, err := client.Query(g.RandomRangeQuery(b.MaxDepth, b.MaxArgs, uint64(b.MinRange), uint64(b.MaxRange)))
		result.Add(time.Since(start), nil)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
