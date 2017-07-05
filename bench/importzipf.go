package bench

import (
	"fmt"
	"io"
	"math/rand"

	"context"

	"github.com/pilosa/go-pilosa"
)

type ImportZipf struct {
	Name         string `json:"name"`
	BaseRowID    int64  `json:"base-row-id"`
	BaseColumnID int64  `json:"base-column-id"`
	MaxRowID     int64  `json:"max-row-id"`
	MaxColumnID  int64  `json:"max-column-id"`
	Index        string `json:"index"`
	Frame        string `json:"frame"`
	Iterations   int64  `json:"iterations"`
	Seed         int64  `json:"seed"`
	BufferSize   uint
	rng          *rand.Rand
	HasClient
}

// Init generates import data based on
func (b *ImportZipf) Init(hosts []string, agentNum int) error {
	if len(hosts) == 0 {
		return fmt.Errorf("Need at least one host")
	}
	b.Name = "import-zipf"
	b.Seed = b.Seed + int64(agentNum)
	b.rng = rand.New(rand.NewSource(b.Seed))
	err := b.HasClient.Init(hosts, agentNum)
	if err != nil {
		return fmt.Errorf("client init: %v", err)
	}
	return b.InitIndex(b.Index, b.Frame)
}

// Run runs the Import benchmark
func (b *ImportZipf) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	results["index"] = b.Index
	bitIterator := b.NewZipfBitIterator()
	err := b.HasClient.Import(b.Index, b.Frame, bitIterator, b.BufferSize)
	if err != nil {
		results["error"] = fmt.Errorf("running go client import: %v", err)
	}
	results["actual-iterations"] = bitIterator.iterations
	return results
}

type ZipfBitIterator struct {
	iterations int64
	bitnum     int64
	maxbitnum  int64
	baserow    int64
	maxrow     int64
	basecol    int64
	maxcol     int64
	avgdelta   float64
	lambda     float64
	rng        *rand.Rand
}

func (b *ImportZipf) NewZipfBitIterator() *ZipfBitIterator {
	z := &ZipfBitIterator{}
	z.maxbitnum = (b.MaxRowID - b.BaseRowID + 1) * (b.MaxColumnID - b.BaseColumnID + 1)
	z.avgdelta = float64(z.maxbitnum) / float64(b.Iterations)
	z.lambda = 1.0 / z.avgdelta
	z.rng = b.rng
	z.iterations, z.baserow, z.basecol, z.maxrow, z.maxcol = b.Iterations, b.BaseRowID, b.BaseColumnID, b.MaxRowID, b.MaxColumnID
	return z
}

func (z *ZipfBitIterator) NextBit() (pilosa.Bit, error) {
	delta := z.rng.ExpFloat64() / z.lambda
	z.bitnum = int64(float64(z.bitnum) + delta)
	if z.bitnum > z.maxbitnum {
		return pilosa.Bit{}, io.EOF
	}
	bit := pilosa.Bit{}
	z.iterations++
	bit.RowID = uint64((z.bitnum / (z.maxcol - z.basecol + 1)) + z.baserow)
	bit.ColumnID = uint64(z.bitnum%(z.maxrow-z.baserow+1) + z.basecol)
	return bit, nil
}

// // GenerateImportCSV writes a generated csv to 'w' which is in the form pilosa/ctl expects for imports.
// func (b *ImportZipf) GenerateImportZipfCSV(w io.Writer) (int64, error) {
// 	maxbitnum := (b.MaxRowID - b.BaseRowID + 1) * (b.MaxColumnID - b.BaseColumnID + 1)
// 	avgdelta := float64(maxbitnum) / float64(b.Iterations)
// 	lambda := 1.0 / avgdelta
// 	var bitnum int64 = 0
// 	var iterations int64 = 0
// 	for {
// 		delta := b.rng.ExpFloat64() / lambda
// 		bitnum = int64(float64(bitnum) + delta)
// 		if bitnum > maxbitnum {
// 			break
// 		}
// 		iterations++
// 		rowID := (bitnum / (b.MaxColumnID - b.BaseColumnID + 1)) + b.BaseRowID
// 		colID := bitnum%(b.MaxRowID-b.BaseRowID+1) + b.BaseColumnID
// 		_, err := w.Write([]byte(fmt.Sprintf("%d,%d\n", rowID, colID)))
// 		if err != nil {
// 			return iterations, fmt.Errorf("GenerateImportZipfCSV, writing: %v", err)
// 		}
// 	}
// 	return iterations, nil
// }
