package bench

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Zipf sets random bits according to the Zipf-Mandelbrot distribution.
// This distribution accepts two parameters, Exponent and Ratio, for both rows and columns.
// It also uses PermutationGenerator to permute IDs randomly.
type Zipf struct {
	HasClient
	Name           string  `json:"name"`
	BaseRowID      int64   `json:"base-row-id"`
	BaseColumnID   int64   `json:"base-column-id"`
	RowIDRange     int64   `json:"row-id-range"`
	ColumnIDRange  int64   `json:"column-id-range"`
	Iterations     int     `json:"iterations"`
	Seed           int64   `json:"seed"`
	Index          string  `json:"index"`
	Frame          string  `json:"frame"`
	RowExponent    float64 `json:"row-exponent"`
	RowRatio       float64 `json:"row-ratio"`
	ColumnExponent float64 `json:"column-exponent"`
	ColumnRatio    float64 `json:"column-ratio"`
	Operation      string  `json:"operation"`
	rowRng         *rand.Zipf
	columnRng      *rand.Zipf
	rowPerm        *PermutationGenerator
	columnPerm     *PermutationGenerator
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

// Init sets up the benchmark based on the agent number and initializes the
// client.
func (b *Zipf) Init(hosts []string, agentNum int) error {
	b.Name = "zipf"
	b.Seed = b.Seed + int64(agentNum)
	rnd := rand.New(rand.NewSource(b.Seed))
	rowOffset := getZipfOffset(b.RowIDRange, b.RowExponent, b.RowRatio)
	b.rowRng = rand.NewZipf(rnd, b.RowExponent, rowOffset, uint64(b.RowIDRange-1))
	columnOffset := getZipfOffset(b.ColumnIDRange, b.ColumnExponent, b.ColumnRatio)
	b.columnRng = rand.NewZipf(rnd, b.ColumnExponent, columnOffset, uint64(b.ColumnIDRange-1))

	b.rowPerm = NewPermutationGenerator(b.RowIDRange, b.Seed)
	b.columnPerm = NewPermutationGenerator(b.ColumnIDRange, b.Seed+1)

	if b.Operation != "set" && b.Operation != "clear" {
		return fmt.Errorf("Unsupported operation: \"%s\" (must be \"set\" or \"clear\")", b.Operation)
	}
	err := initIndex(hosts[0], b.Index, b.Frame)
	if err != nil {
		fmt.Println(err)
	}

	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the Zipf benchmark
func (b *Zipf) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	if b.client == nil {
		results["error"] = fmt.Errorf("No client set for Zipf")
		return results
	}
	operation := "SetBit"
	if b.Operation == "clear" {
		operation = "ClearBit"
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		// generate IDs from Zipf distribution
		rowIDOriginal := b.rowRng.Uint64()
		profIDOriginal := b.columnRng.Uint64()
		// permute IDs randomly, but repeatably
		rowID := b.rowPerm.Next(int64(rowIDOriginal))
		profID := b.columnPerm.Next(int64(profIDOriginal))

		query := fmt.Sprintf("%s(frame='%s', rowID=%d, columnID=%d)", operation, b.Frame, b.BaseRowID+int64(rowID), b.BaseColumnID+int64(profID))
		start = time.Now()
		_, err := b.client.ExecuteQuery(ctx, b.Index, query, true)
		if err != nil {
			results["error"] = err.Error()
			return results
		}
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
