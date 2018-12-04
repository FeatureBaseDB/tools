package bench

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/pilosa/go-pilosa"
)

// ZipfBenchmark sets random bits according to the Zipf-Mandelbrot distribution.
// This distribution accepts two parameters, Exponent and Ratio, for both rows and columns.
// It also uses PermutationGenerator to permute IDs randomly.
type ZipfBenchmark struct {
	Name           string  `json:"name"`
	MinRowID       int64   `json:"min-row-id"`
	MinColumnID    int64   `json:"min-column-id"`
	MaxRowID       int64   `json:"max-row-id"`
	MaxColumnID    int64   `json:"max-column-id"`
	Iterations     int     `json:"iterations"`
	Seed           int64   `json:"seed"`
	Index          string  `json:"index"`
	Field          string  `json:"field"`
	RowExponent    float64 `json:"row-exponent"`
	RowRatio       float64 `json:"row-ratio"`
	ColumnExponent float64 `json:"column-exponent"`
	ColumnRatio    float64 `json:"column-ratio"`
	Operation      string  `json:"operation"`

	Logger *log.Logger `json:"-"`
}

// NewZipfBenchmark returns a new instance of ZipfBenchmark.
func NewZipfBenchmark() *ZipfBenchmark {
	return &ZipfBenchmark{
		Name:   "zipf",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the Zipf benchmark
func (b *ZipfBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// Initialize schema.
	_, field, err := ensureSchema(client, b.Index, b.Field)
	if err != nil {
		return result, err
	}

	seed := b.Seed + int64(agentNum)
	rowOffset := getZipfOffset(b.MaxRowID-b.MinRowID, b.RowExponent, b.RowRatio)
	rowRand := rand.NewZipf(rand.New(rand.NewSource(seed)), b.RowExponent, rowOffset, uint64(b.MaxRowID-b.MinRowID-1))
	columnOffset := getZipfOffset(b.MaxColumnID-b.MinColumnID, b.ColumnExponent, b.ColumnRatio)
	columnRand := rand.NewZipf(rand.New(rand.NewSource(seed)), b.ColumnExponent, columnOffset, uint64(b.MaxColumnID-b.MinColumnID-1))
	rowPerm := NewPermutationGenerator(b.MaxRowID-b.MinRowID, seed)
	columnPerm := NewPermutationGenerator(b.MaxColumnID-b.MinColumnID, seed+1)

	for n := 0; n < b.Iterations; n++ {
		// generate IDs from Zipf distribution
		rowIDOriginal := rowRand.Uint64()
		profIDOriginal := columnRand.Uint64()

		// permute IDs randomly, but repeatably
		rowID := rowPerm.Next(int64(rowIDOriginal))
		profID := columnPerm.Next(int64(profIDOriginal))

		var q pilosa.PQLQuery
		switch b.Operation {
		case "set":
			q = field.Set(b.MinRowID+int64(rowID), b.MinColumnID+int64(profID))
		case "clear":
			q = field.Clear(b.MinRowID+int64(rowID), b.MinColumnID+int64(profID))
		default:
			return result, fmt.Errorf("Unsupported operation: \"%s\" (must be \"set\" or \"clear\")", b.Operation)
		}

		start := time.Now()
		_, err := client.Query(q)
		result.Add(time.Since(start), nil)
		if err != nil {
			return result, err
		}
	}
	return result, err
}

// Offset is the true parameter used by the Zipf distribution, but the ratio,
// as defined here, is a simpler, readable way to define the distribution.
// Offset is in [1, inf), and its meaning depends on N (a pain for updating benchmark configs)
// ratio is in (0, 1), and its meaning does not depend on N.
// it is the ratio of the lowest probability in the distribution to the highest.
// ratio=0.01 corresponds to a very small offset - the most skewed distribution for a given pair (N, exp)
// ratio=0.99 corresponds to a very large offset - the most nearly uniform distribution for a given (N, exp)
func getZipfOffset(N int64, exp, ratio float64) float64 {
	z := math.Pow(ratio, 1/exp)
	return z * float64(N-1) / (1 - z)
}
