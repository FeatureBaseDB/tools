package bench

import (
	"fmt"

	"context"
	"time"
)

// DiagonalSetBits sets bits with increasing column id and row id.
type DiagonalSetBits struct {
	HasClient
	Name         string `json:"name"`
	BaseRowID    int    `json:"base-row-id"`
	BaseColumnID int    `json:"base-column-id"`
	Iterations   int    `json:"iterations"`
	Index        string `json:"index"`
	Frame        string `json:"frame"`
}

// Init sets up the pilosa client and modifies the configured values based on
// the agent num.
func (b *DiagonalSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "diagonal-set-bits"
	b.BaseRowID = b.BaseRowID + (agentNum * b.Iterations)
	b.BaseColumnID = b.BaseColumnID + (agentNum * b.Iterations)
	err := b.HasClient.Init(hosts, agentNum)
	if err != nil {
		return err
	}
	return b.InitIndex(b.Index, b.Frame)
}

// Run runs the DiagonalSetBits benchmark
func (b *DiagonalSetBits) Run(ctx context.Context) *Result {
	results := NewResult()
	if b.client == nil {
		results.Error = fmt.Errorf("No client set for DiagonalSetBits")
		return results
	}
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		query := fmt.Sprintf("SetBit(frame='%s', rowID=%d, columnID=%d)", b.Frame, b.BaseRowID+n, b.BaseColumnID+n)
		start = time.Now()
		_, err := b.ExecuteQuery(ctx, b.Index, query)
		results.Add(time.Since(start), nil)
		if err != nil {
			results.Error = err
			return results
		}
	}
	return results
}
