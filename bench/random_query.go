package bench

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/pilosa/go-pilosa"
)

var _ Benchmark = (*BasicQueryBenchmark)(nil)

// RandomQueryBenchmark queries randomly and deterministically based on a seed.
type RandomQueryBenchmark struct {
	Name       string `json:"name"`
	MaxDepth   int    `json:"max-depth"`
	MaxArgs    int    `json:"max-args"`
	MaxN       int    `json:"max-n"`
	MinRowID   int64  `json:"min-row-id"`
	MaxRowID   int64  `json:"max-row-id"`
	Iterations int    `json:"iterations"`
	Seed       int64  `json:"seed"`
	Index      string `json:"index"`
	Field      string `json:"field"`

	Logger *log.Logger `json:"-"`
}

// NewRandomQueryBenchmark returns a new instance of RandomQueryBenchmark.
func NewRandomQueryBenchmark() *RandomQueryBenchmark {
	return &RandomQueryBenchmark{
		Name:   "random-query",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the RandomQuery benchmark
func (b *RandomQueryBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// Initialize schema.
	index, field, err := ensureSchema(client, b.Index, b.Field)
	if err != nil {
		return result, err
	}

	g := NewQueryGenerator(index, field, b.Seed+int64(agentNum))
	for n := 0; n < b.Iterations; n++ {
		start := time.Now()
		_, err := client.Query(g.Random(b.MaxN, b.MaxDepth, b.MaxArgs, uint64(b.MinRowID), uint64(b.MaxRowID-b.MinRowID)))
		result.Add(time.Since(start), nil)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
