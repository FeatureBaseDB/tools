package bench

import (
	"fmt"

	"context"
	"math/rand"
	"time"
)

// MultipleSetBits sets multiple bits.
type MultipleSetBits struct {
	HasClient
	Name          string `json:"name"`
	BaseRowID     int64  `json:"base-row-id"`
	BaseColumnID  int64  `json:"base-column-id"`
	RowIDRange    int64  `json:"row-id-range"`
	ColumnIDRange int64  `json:"column-id-range"`
	Iterations    int    `json:"iterations"`
	Seed          int64  `json:"seed"`
	Index         string `json:"index"`
	Frame         string `json:"frame"`
	BatchSize     int    `json:"batch-size"`
}

// Init adds the agent num to the random seed and initializes the client.
func (b *MultipleSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "multiple-setbit"
	b.Seed = b.Seed + int64(agentNum)
	err := b.HasClient.Init(hosts, agentNum)
	if err != nil {
		return err
	}
	return b.InitIndex(b.Index, b.Frame)
}

// Run runs the MultipleSetBits benchmark
func (b *MultipleSetBits) Run(ctx context.Context) *Result {
	src := rand.NewSource(b.Seed)
	rng := rand.New(src)
	results := NewResult()
	if b.client == nil {
		results.err = fmt.Errorf("No client set for MultipleSetBits")
		return results
	}

	var queries []string
	for i := 0; i < b.BatchSize; i++ {
		rowID := rng.Int63n(b.RowIDRange)
		profID := rng.Int63n(b.ColumnIDRange)
		query := fmt.Sprintf("SetBit(frame='%s', rowID=%d, columnID=%d)", b.Frame, b.BaseRowID+rowID, b.BaseColumnID+profID)
		queries = append(queries, query)
	}

	for n := 0; n < b.Iterations; n++ {
		start := time.Now()
		_, err := b.ExecuteBatchQuery(ctx, b.Index, queries)
		results.Add(time.Since(start), nil)
		if err != nil {
			results.err = err
			return results
		}
	}
	return results
}
