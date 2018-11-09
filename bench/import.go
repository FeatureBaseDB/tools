package bench

import (
	"context"
	"io"
	"log"
	"math/rand"
	"os"

	"github.com/pilosa/go-pilosa"
)

var _ Benchmark = (*ImportBenchmark)(nil)

type ImportBenchmark struct {
	Name         string `json:"name"`
	MinRowID     int64  `json:"min-row-id"`
	MinColumnID  int64  `json:"min-column-id"`
	MaxRowID     int64  `json:"max-row-id"`
	MaxColumnID  int64  `json:"max-column-id"`
	Index        string `json:"index"`
	Field        string `json:"field"`
	Iterations   int64  `json:"iterations"`
	Seed         int64  `json:"seed"`
	Distribution string `json:"distribution"`
	BufferSize   int    `json:"-"`

	Logger *log.Logger `json:"-"`
}

// NewImportBenchmark returns a new instance of ImportBenchmark.
func NewImportBenchmark() *ImportBenchmark {
	return &ImportBenchmark{
		Name:   "import",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the Import benchmark
func (b *ImportBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// Initialize schema.
	_, field, err := ensureSchema(client, b.Index, b.Field)
	if err != nil {
		return result, err
	}

	itr := b.RecordIterator(b.Seed + int64(agentNum))
	err = client.ImportField(field, itr, pilosa.OptImportBatchSize(b.BufferSize))
	result.Extra["actual-iterations"] = itr.actualIterations
	result.Extra["avgdelta"] = itr.avgdelta
	return result, err
}

func (b *ImportBenchmark) RecordIterator(seed int64) *RecordIterator {
	rand := rand.New(rand.NewSource(seed))

	itr := NewRecordIterator()
	itr.maxbitnum = (b.MaxRowID - b.MinRowID + 1) * (b.MaxColumnID - b.MinColumnID + 1)
	itr.avgdelta = float64(itr.maxbitnum) / float64(b.Iterations)
	itr.minrow, itr.mincol, itr.maxrow, itr.maxcol = b.MinRowID, b.MinColumnID, b.MaxRowID, b.MaxColumnID

	if b.Distribution == "exponential" {
		itr.lambda = 1.0 / itr.avgdelta
		itr.fdelta = func(itr *RecordIterator) float64 {
			return rand.ExpFloat64() / itr.lambda
		}
	} else { // if b.Distribution == "uniform" {
		itr.fdelta = func(itr *RecordIterator) float64 {
			return rand.Float64() * itr.avgdelta * 2
		}
	}
	return itr
}

func NewRecordIterator() *RecordIterator {
	return &RecordIterator{}
}

type RecordIterator struct {
	actualIterations int64
	bitnum           int64
	maxbitnum        int64
	minrow           int64
	maxrow           int64
	mincol           int64
	maxcol           int64
	avgdelta         float64
	lambda           float64
	rand             *rand.Rand
	fdelta           func(z *RecordIterator) float64
}

func (itr *RecordIterator) NextRecord() (pilosa.Record, error) {
	delta := itr.fdelta(itr)
	if delta < 1.0 {
		delta = 1.0
	}
	itr.bitnum = int64(float64(itr.bitnum) + delta)
	if itr.bitnum > itr.maxbitnum {
		return pilosa.Column{}, io.EOF
	}
	itr.actualIterations++
	return pilosa.Column{
		RowID:    uint64((itr.bitnum / (itr.maxcol - itr.mincol + 1)) + itr.minrow),
		ColumnID: uint64(itr.bitnum%(itr.maxcol-itr.mincol+1) + itr.mincol),
	}, nil
}
