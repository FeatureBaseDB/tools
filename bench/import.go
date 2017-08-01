package bench

import (
	"fmt"
	"io"
	"math/rand"

	"context"

	"github.com/pilosa/go-pilosa"
)

type Import struct {
	Name         string `json:"name"`
	BaseRowID    int64  `json:"base-row-id"`
	BaseColumnID int64  `json:"base-column-id"`
	MaxRowID     int64  `json:"max-row-id"`
	MaxColumnID  int64  `json:"max-column-id"`
	Index        string `json:"index"`
	Frame        string `json:"frame"`
	Iterations   int64  `json:"iterations"`
	Seed         int64  `json:"seed"`
	Distribution string `json:"distribution"`
	BufferSize   uint
	rng          *rand.Rand
	HasClient
}

// Init generates import data based on
func (b *Import) Init(hosts []string, agentNum int) error {
	if len(hosts) == 0 {
		return fmt.Errorf("Need at least one host")
	}
	b.Name = "import"
	b.Seed = b.Seed + int64(agentNum)
	b.rng = rand.New(rand.NewSource(b.Seed))
	err := b.HasClient.Init(hosts, agentNum)
	if err != nil {
		return fmt.Errorf("client init: %v", err)
	}
	return b.InitIndex(b.Index, b.Frame)
}

// Run runs the Import benchmark
func (b *Import) Run(ctx context.Context) *Result {
	results := NewResult()
	bitIterator := b.NewBitIterator()
	err := b.HasClient.Import(b.Index, b.Frame, bitIterator, b.BufferSize)
	if err != nil {
		results.err = fmt.Errorf("running go client import: %v", err)
	}
	results.Extra["actual-iterations"] = bitIterator.actualIterations
	results.Extra["avgdelta"] = bitIterator.avgdelta
	return results
}

type BitIterator struct {
	actualIterations int64
	bitnum           int64
	maxbitnum        int64
	baserow          int64
	maxrow           int64
	basecol          int64
	maxcol           int64
	avgdelta         float64
	lambda           float64
	rng              *rand.Rand
	fdelta           func(z *BitIterator) float64
}

func (b *Import) NewBitIterator() *BitIterator {
	z := &BitIterator{}
	z.rng = b.rng
	z.maxbitnum = (b.MaxRowID - b.BaseRowID + 1) * (b.MaxColumnID - b.BaseColumnID + 1)
	z.avgdelta = float64(z.maxbitnum) / float64(b.Iterations)
	z.baserow, z.basecol, z.maxrow, z.maxcol = b.BaseRowID, b.BaseColumnID, b.MaxRowID, b.MaxColumnID

	if b.Distribution == "exponential" {
		z.lambda = 1.0 / z.avgdelta
		z.fdelta = func(z *BitIterator) float64 {
			return z.rng.ExpFloat64() / z.lambda
		}
	} else { // if b.Distribution == "uniform" {
		z.fdelta = func(z *BitIterator) float64 {
			return z.rng.Float64() * z.avgdelta * 2
		}
	}
	return z
}

func (z *BitIterator) NextBit() (pilosa.Bit, error) {
	delta := z.fdelta(z)
	z.bitnum = int64(float64(z.bitnum) + delta)
	if z.bitnum > z.maxbitnum {
		return pilosa.Bit{}, io.EOF
	}
	bit := pilosa.Bit{}
	z.actualIterations++
	bit.RowID = uint64((z.bitnum / (z.maxcol - z.basecol + 1)) + z.baserow)
	bit.ColumnID = uint64(z.bitnum%(z.maxcol-z.basecol+1) + z.basecol)
	return bit, nil
}
