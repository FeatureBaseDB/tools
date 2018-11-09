package bench

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/pilosa/go-pilosa"
)

var _ Benchmark = (*RandomSetBenchmark)(nil)

// RandomSetBenchmark sets bits randomly and deterministically based on a seed.
type RandomSetBenchmark struct {
	Name          string `json:"name"`
	MinRowID      int64  `json:"min-row-id"`
	MaxRowID      int64  `json:"max-row-id"`
	MinColumnID   int64  `json:"min-column-id"`
	MaxColumnID   int64  `json:"max-column-id"`
	Iterations    int    `json:"iterations"`
	BatchSize     int    `json:"batch-size"`
	Seed          int64  `json:"seed"`
	NumAttrs      int    `json:"num-attrs"`
	NumAttrValues int    `json:"num-attr-values"`
	Index         string `json:"index"`
	Field         string `json:"field"`

	Logger *log.Logger `json:"-"`
}

// NewRandomSetBenchmark returns a new instance of RandomSetBenchmark.
func NewRandomSetBenchmark() *RandomSetBenchmark {
	return &RandomSetBenchmark{
		Name:   "random-set",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the benchmark.
func (b *RandomSetBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	if b.BatchSize <= 0 {
		return result, fmt.Errorf("batch size must be greater than 0, currently: %d", b.BatchSize)
	}

	// Initialize schema.
	index, field, err := ensureSchema(client, b.Index, b.Field)
	if err != nil {
		return result, err
	}

	rand := rand.New(rand.NewSource(b.Seed))
	const letters = "abcdefghijklmnopqrstuvwxyz"
	for n := 0; n < b.Iterations; {
		var a []pilosa.PQLQuery
		for i := 0; i < b.BatchSize && n < b.Iterations; i, n = i+1, n+1 {
			rowID := rand.Int63n(b.MaxRowID - b.MinRowID)
			columnID := rand.Int63n(b.MaxColumnID - b.MinColumnID)

			a = append(a, field.Set(b.MinRowID+rowID, b.MinColumnID+columnID))

			if b.NumAttrs > 0 && b.NumAttrValues > 0 {
				attri := rand.Intn(b.NumAttrs)
				key := fmt.Sprintf("%c%d", letters[attri%len(letters)], attri)
				val := rand.Intn(b.NumAttrValues)
				a = append(a, field.SetRowAttrs(rowID, map[string]interface{}{key: val}))
			}
		}

		start := time.Now()
		_, err := client.Query(index.BatchQuery(a...))
		result.Add(time.Since(start), nil)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
