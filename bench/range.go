package bench

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// RangeQuery runs Range query randomly
type RangeQuery struct {
	HasClient
	Name       string   `json:"name"`
	MaxDepth   int      `json:"max-depth"`
	MaxArgs    int      `json:"max-args"`
	MaxN       int      `json:"max-n"`
	MinRange   int64    `json:"min-range"`
	MaxRange   int64    `json:"max-range"`
	Iterations int      `json:"iterations"`
	Seed       int64    `json:"seed"`
	Frame      string   `json:"frame"`
	Index      string   `json:"index"`
	Fields     []string `json:"field"`
}

// Init adds the agent num to the random seed and initializes the client.
func (b *RangeQuery) Init(hosts []string, agentNum int) error {
	b.Name = "range-query"
	b.Seed = b.Seed + int64(agentNum)
	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the RandomQuery benchmark
func (b *RangeQuery) Run(ctx context.Context) *Result {
	src := rand.NewSource(b.Seed)
	rng := rand.New(src)
	results := NewResult()
	if b.client == nil {
		results.err = fmt.Errorf("No client set for RangeQuery")
		return results
	}
	qm := NewQueryGenerator(b.Seed)
	qm.IDToFrameFn = func(id uint64) string { return b.Frame }

	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		rangeValue := rng.Int63n(b.MaxRange - b.MinRange)
		var field int
		if len(b.Fields) > 1 {
			field = rng.Intn(len(b.Fields) - 1)
		} else {
			field = 0
		}
		query := fmt.Sprintf("Range(frame='%s', %s<%d)", b.Frame, b.Fields[field], rangeValue)

		start = time.Now()
		_, err := b.ExecuteQuery(ctx, b.Index, query)
		results.Add(time.Since(start), nil)
		if err != nil {
			results.err = fmt.Errorf("Executing '%s', err: %v", query, err)
			return results
		}
	}
	return results
}
