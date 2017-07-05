package bench

import (
	"fmt"
	"io"
	"math/rand"

	"context"

	"github.com/pilosa/pilosa/ctl"
)

type ImportZipf struct {
	Name          string `json:"name"`
	BaseBitmapID  int64  `json:"base-bitmap-id"`
	BaseProfileID int64  `json:"base-profile-id"`
	MaxBitmapID   int64  `json:"max-bitmap-id"`
	MaxProfileID  int64  `json:"max-profile-id"`
	Iterations    int    `json:"iterations"`
	Seed          int64  `json:"seed"`
	importStdin   io.WriteCloser
	rng           *rand.Rand

	*ctl.ImportCommand
}

// NewImport returns an Import Benchmark which pilosa/ctl importer configured.
func NewImportZipf(stdin io.Reader, stdout, stderr io.Writer) *ImportZipf {
	importStdinR, importStdinW := io.Pipe()
	bm := &ImportZipf{
		importStdin:   importStdinW,
		ImportCommand: ctl.NewImportCommand(importStdinR, stdout, stderr),
	}
	return bm
}

// Init generates import data based on
func (b *ImportZipf) Init(hosts []string, agentNum int) error {
	if len(hosts) == 0 {
		return fmt.Errorf("Need at least one host")
	}
	b.Name = "import-zipf"
	b.Host = hosts[0]
	// generate csv data
	b.Seed = b.Seed + int64(agentNum)
	b.rng = rand.New(rand.NewSource(b.Seed))

	b.Paths = []string{"-"}
	err := initIndex(b.Host, b.Index, b.Frame)
	if err != nil {
		return fmt.Errorf("initIndex: %v", err)
	}

	return nil
}

// Run runs the Import benchmark
func (b *ImportZipf) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	results["index"] = b.Index
	done := make(chan error)
	go func() {
		err := b.ImportCommand.Run(ctx)
		if err != nil {
			done <- fmt.Errorf("ImportCommand.Run: %v", err)
		}
		close(done)
	}()
	iters, err := b.GenerateImportZipfCSV(b.importStdin)
	results["actual-iterations"] = iters
	if err != nil {
		results["error"] = fmt.Sprintf("generating import csv: %v", err)
	}
	err = b.importStdin.Close()
	if err != nil {
		if err != nil {
			results["error"] = fmt.Sprintf("closing pipe to importer: %v", err)
		}
	}

	err = <-done
	if err != nil {
		results["error"] = err.Error()
	}
	return results
}

// GenerateImportCSV writes a generated csv to 'w' which is in the form pilosa/ctl expects for imports.
func (b *ImportZipf) GenerateImportZipfCSV(w io.Writer) (int64, error) {
	maxbitnum := (b.MaxBitmapID - b.BaseBitmapID + 1) * (b.MaxProfileID - b.BaseProfileID + 1)
	avgdelta := float64(maxbitnum) / float64(b.Iterations)
	lambda := 1.0 / avgdelta
	var bitnum int64 = 0
	var iterations int64 = 0
	for {
		delta := b.rng.ExpFloat64() / lambda
		bitnum = int64(float64(bitnum) + delta)
		if bitnum > maxbitnum {
			break
		}
		iterations++
		rowID := (bitnum / (b.MaxProfileID - b.BaseProfileID + 1)) + b.BaseBitmapID
		colID := bitnum%(b.MaxBitmapID-b.BaseBitmapID+1) + b.BaseProfileID
		_, err := w.Write([]byte(fmt.Sprintf("%d,%d\n", rowID, colID)))
		if err != nil {
			return iterations, fmt.Errorf("GenerateImportZipfCSV, writing: %v", err)
		}
	}
	return iterations, nil
}
