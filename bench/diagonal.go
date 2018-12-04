package bench

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/pilosa/go-pilosa"
)

// DiagonalSetBitsBenchmark sets bits with increasing column id and row id.
type DiagonalSetBitsBenchmark struct {
	Name        string `json:"name"`
	MinRowID    int    `json:"min-row-id"`
	MinColumnID int    `json:"min-column-id"`
	Iterations  int    `json:"iterations"`
	Index       string `json:"index"`
	Field       string `json:"field"`

	Logger *log.Logger `json:"-"`
}

// NewDiagonalSetBitsBenchmark returns a new instance of DiagonalSetBitsBenchmark.
func NewDiagonalSetBitsBenchmark() *DiagonalSetBitsBenchmark {
	return &DiagonalSetBitsBenchmark{
		Name:   "diagonal-set-bits",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the benchmark.
func (b *DiagonalSetBitsBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// Initialize schema.
	_, field, err := ensureSchema(client, b.Index, b.Field)
	if err != nil {
		return result, err
	}

	minRowID := b.MinRowID + (agentNum * b.Iterations)
	minColumnID := b.MinColumnID + (agentNum * b.Iterations)

	for n := 0; n < b.Iterations; n++ {
		start := time.Now()
		_, err := client.Query(field.Set(minRowID+n, minColumnID+n))
		result.Add(time.Since(start), nil)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
