package bench

import (
	"context"
	"io"
	"log"
	"math/rand"
	"os"

	"github.com/pilosa/go-pilosa"
)

var _ Benchmark = (*ImportRangeBenchmark)(nil)

type ImportRangeBenchmark struct {
	Name         string `json:"name"`
	MinValue     int64  `json:"min-value"`
	MinColumnID  int64  `json:"min-column-id"`
	MaxValue     int64  `json:"max-value"`
	MaxColumnID  int64  `json:"max-column-id"`
	Index        string `json:"index"`
	Field        string `json:"field"`
	Row          string `json:"row"`
	Iterations   int64  `json:"iterations"`
	Seed         int64  `json:"seed"`
	Distribution string `json:"distribution"`
	BufferSize   int    `json:"-"`

	Logger *log.Logger `json:"-"`
}

// NewImportRangeBenchmark returns a new instance of ImportRangeBenchmark.
func NewImportRangeBenchmark() *ImportRangeBenchmark {
	return &ImportRangeBenchmark{
		Name:   "import-range",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the benchmark.
func (b *ImportRangeBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// Initialize schema.
	_, field, err := ensureSchema(client, b.Index, b.Field, pilosa.OptFieldTypeInt(b.MinValue, b.MaxValue))
	if err != nil {
		return result, err
	}

	itr := b.ValueIterator(b.Seed + int64(agentNum))
	err = client.ImportField(field, itr, pilosa.OptImportBatchSize(b.BufferSize))
	result.Extra["actual-iterations"] = itr.actualIterations
	result.Extra["avgdelta"] = itr.avgdelta
	return result, err
}

func (b *ImportRangeBenchmark) ValueIterator(seed int64) *ValueIterator {
	rand := rand.New(rand.NewSource(seed))

	itr := NewValueIterator()
	itr.maxbitnum = (b.MaxValue - b.MinValue + 1) * (b.MaxColumnID - b.MinColumnID + 1)
	itr.avgdelta = float64(itr.maxbitnum) / float64(b.Iterations)
	itr.minvalue, itr.mincol, itr.maxvalue, itr.maxcol = b.MinValue, b.MinColumnID, b.MaxValue, b.MaxColumnID

	if b.Distribution == "exponential" {
		itr.lambda = 1.0 / itr.avgdelta
		itr.fdelta = func(itr *ValueIterator) float64 {
			return rand.ExpFloat64() / itr.lambda
		}
	} else { // if b.Distribution == "uniform" {
		itr.fdelta = func(itr *ValueIterator) float64 {
			return rand.Float64() * itr.avgdelta * 2
		}
	}
	return itr
}

func NewValueIterator() *ValueIterator {
	return &ValueIterator{}
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
	fdelta           func(itr *ValueIterator) float64
}

func (itr *ValueIterator) NextRecord() (pilosa.Record, error) {
	delta := itr.fdelta(itr)
	if delta < 1.0 {
		delta = 1.0
	}
	itr.bitnum = int64(float64(itr.bitnum) + delta)
	if itr.bitnum > itr.maxbitnum {
		return pilosa.FieldValue{}, io.EOF
	}

	itr.actualIterations++
	return pilosa.FieldValue{
		Value:    int64((itr.bitnum / (itr.maxcol - itr.mincol + 1)) + itr.minvalue),
		ColumnID: uint64(itr.bitnum%(itr.maxcol-itr.mincol+1) + itr.mincol),
	}, nil
}
