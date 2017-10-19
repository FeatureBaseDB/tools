package bench

import (
	"context"
	"fmt"
	"io"
	"math/rand"

	pcli "github.com/pilosa/go-pilosa"
)

type ImportRange struct {
	Name         string `json:"name"`
	MinValue     int64  `json:"min-value"`
	MinColumnID  int64  `json:"min-column-id"`
	MaxValue     int64  `json:"max-value"`
	MaxColumnID  int64  `json:"max-column-id"`
	Index        string `json:"index"`
	Frame        string `json:"frame"`
	Field        string `json:"field"`
	Iterations   int64  `json:"iterations"`
	Seed         int64  `json:"seed"`
	Distribution string `json:"distribution"`
	BufferSize   uint
	rng          *rand.Rand
	HasClient
}

type ValueIterator struct {
	actualIterations int64
	bitnum           int64
	maxbitnum        int64
	minvalue         int64
	maxvalue         int64
	mincol           int64
	maxcol           int64
	avgdelta         float64
	lambda           float64
	rng              *rand.Rand
	fdelta           func(z *ValueIterator) float64
}

// Init create client and range frame.
func (b *ImportRange) Init(hosts []string, agentNum int, clientOptions *pcli.ClientOptions) error {
	if len(hosts) == 0 {
		return fmt.Errorf("Need at least one host")
	}
	b.Name = "import-range"
	b.Seed = b.Seed + int64(agentNum)
	b.rng = rand.New(rand.NewSource(b.Seed))
	err := b.HasClient.Init(hosts, agentNum, clientOptions)
	if err != nil {
		return fmt.Errorf("client init: %v", err)
	}

	return b.InitRange(b.Index, b.Frame, b.Field, b.MinValue, b.MaxValue)
}

// Run runs the ImportRange benchmark
func (b *ImportRange) Run(ctx context.Context) *Result {
	results := NewResult()

	valueIterator := b.NewValueIterator()
	err := b.HasClient.ImportRange(b.Index, b.Frame, b.Field, valueIterator, b.BufferSize)
	if err != nil {
		results.err = fmt.Errorf("running go client import: %v", err)
	}
	results.Extra["actual-iterations"] = valueIterator.actualIterations
	results.Extra["avgdelta"] = valueIterator.avgdelta
	return results
}

func (b *ImportRange) NewValueIterator() *ValueIterator {
	z := &ValueIterator{}
	z.rng = b.rng
	z.maxbitnum = (b.MaxValue - b.MinValue + 1) * (b.MaxColumnID - b.MinColumnID + 1)
	z.avgdelta = float64(z.maxbitnum) / float64(b.Iterations)
	z.minvalue, z.mincol, z.maxvalue, z.maxcol = b.MinValue, b.MinColumnID, b.MaxValue, b.MaxColumnID

	if b.Distribution == "exponential" {
		z.lambda = 1.0 / z.avgdelta
		z.fdelta = func(z *ValueIterator) float64 {
			return z.rng.ExpFloat64() / z.lambda
		}
	} else { // if b.Distribution == "uniform" {
		z.fdelta = func(z *ValueIterator) float64 {
			return z.rng.Float64() * z.avgdelta * 2
		}
	}
	return z
}

func (z *ValueIterator) NextValue() (pcli.FieldValue, error) {
	delta := z.fdelta(z)
	if delta < 1.0 {
		delta = 1.0
	}
	z.bitnum = int64(float64(z.bitnum) + delta)
	if z.bitnum > z.maxbitnum {
		return pcli.FieldValue{}, io.EOF
	}

	field := pcli.FieldValue{}
	z.actualIterations++
	field.Value = uint64((z.bitnum / (z.maxcol - z.mincol + 1)) + z.minvalue)
	field.ColumnID = uint64(z.bitnum%(z.maxcol-z.mincol+1) + z.mincol)
	return field, nil
}
