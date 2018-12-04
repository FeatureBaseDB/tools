package bench

import (
	"context"
	"log"
	"os"

	"github.com/pilosa/go-pilosa"
)

// SliceWidthBenchmark helps importing data based on slice-width and data density.
// a single slice on query time.
type SliceWidthBenchmark struct {
	Name       string  `json:"name"`
	Index      string  `json:"index"`
	Field      string  `json:"field"`
	BitDensity float64 `json:"bit-density"`
	SliceWidth int64   `json:"slice-width"`
	SliceCount int64   `json:"slice-count"`

	Logger *log.Logger `json:"-"`
}

// NewSliceWidthBenchmark creates slice width benchmark.
func NewSliceWidthBenchmark() *SliceWidthBenchmark {
	return &SliceWidthBenchmark{
		Name:   "slice-width",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the benchmark to import data.
func (b *SliceWidthBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	numColumns := b.SliceWidth * b.SliceCount
	numRows := int64(1000)

	importBenchmark := NewImportBenchmark()
	importBenchmark.MaxRowID = numRows
	importBenchmark.MinColumnID = 0
	importBenchmark.MaxColumnID = numColumns
	importBenchmark.Iterations = int64(float64(numColumns)*b.BitDensity) * numRows
	importBenchmark.Index = b.Index
	importBenchmark.Field = b.Field
	importBenchmark.Distribution = "uniform"
	importBenchmark.BufferSize = 1000000

	result, err := importBenchmark.Run(ctx, client, agentNum)
	result.Configuration = b
	return result, err
}
