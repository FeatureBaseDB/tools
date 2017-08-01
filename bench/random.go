package bench

import (
	"fmt"

	"context"
	"math/rand"
	"time"
)

// RandomSetBits sets bits randomly and deterministically based on a seed.
type RandomSetBits struct {
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
}

// Init adds the agent num to the random seed and initializes the client.
func (b *RandomSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "random-set-bits"
	b.Seed = b.Seed + int64(agentNum)
	err := b.HasClient.Init(hosts, agentNum)
	if err != nil {
		return err
	}
	return b.InitIndex(b.Index, b.Frame)
}

// Run runs the RandomSetBits benchmark
func (b *RandomSetBits) Run(ctx context.Context) *Result {
	src := rand.NewSource(b.Seed)
	rng := rand.New(src)
	results := NewResult()
	if b.client == nil {
		results.err = fmt.Errorf("No client set for RandomSetBits")
		return results
	}
	for n := 0; n < b.Iterations; n++ {
		rowID := rng.Int63n(b.RowIDRange)
		profID := rng.Int63n(b.ColumnIDRange)
		query := fmt.Sprintf("SetBit(frame='%s', rowID=%d, columnID=%d)", b.Frame, b.BaseRowID+rowID, b.BaseColumnID+profID)
		start := time.Now()
		_, err := b.ExecuteQuery(ctx, b.Index, query)
		results.Add(time.Since(start), nil)
		if err != nil {
			results.err = err
			return results
		}
	}
	return results
}
