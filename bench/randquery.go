package bench

import (
	"context"
	"fmt"
	"time"
)

// RandomQuery queries randomly and deterministically based on a seed.
type RandomQuery struct {
	HasClient
	Name       string   `json:"name"`
	MaxDepth   int      `json:"max-depth"`
	MaxArgs    int      `json:"max-args"`
	MaxN       int      `json:"max-n"`
	BaseRowID  int64    `json:"base-row-id"`
	RowIDRange int64    `json:"row-id-range"`
	Iterations int      `json:"iterations"`
	Seed       int64    `json:"seed"`
	Frame      string   `json:"frame"`
	Indexes    []string `json:"indexes"`
}

// Init adds the agent num to the random seed and initializes the client.
func (b *RandomQuery) Init(hosts []string, agentNum int) error {
	b.Name = "random-query"
	b.Seed = b.Seed + int64(agentNum)
	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the RandomQuery benchmark
func (b *RandomQuery) Run(ctx context.Context) *Result {
	results := NewResult()
	if b.client == nil {
		results.Error = fmt.Errorf("No client set for RandomQuery")
		return results
	}
	qm := NewQueryGenerator(b.Seed)
	qm.IDToFrameFn = func(id uint64) string { return b.Frame }

	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		call := qm.Random(b.MaxN, b.MaxDepth, b.MaxArgs, uint64(b.BaseRowID), uint64(b.RowIDRange))
		start = time.Now()
		_, err := b.ExecuteQuery(ctx, b.Indexes[n%len(b.Indexes)], call.String())
		results.Add(time.Since(start), nil)
		if err != nil {
			results.Error = fmt.Errorf("Executing '%s' against '%s', err: %v", call.String(), b.Indexes[n%len(b.Indexes)], err)
			return results
		}
	}
	return results
}
