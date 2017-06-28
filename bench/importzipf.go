package bench

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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

// Usage returns the usage message to be printed.
func (b *ImportZipf) Usage() string {
	return `
import-zipf generates an import file which sets bits according to the Zipf
distribution in both row and column id and imports using pilosa's bulk import
interface.

Agent num affects random seed.

Usage: import-zipf [arguments]

The following arguments are available:

	-base-bitmap-id int
		bits being set will all be greater than this

	-max-bitmap-id int
		bits being set will all be less than this

	-base-profile-id int
		profile id num to start from

	-max-profile-id int
		maximum profile id to generate

	-iterations int
		number of bits to set

	-seed int
		seed for RNG

	-index string
		pilosa index to use

	-frame string
		frame to import into

	-buffer-size int
		buffer size for importer to use

`[1:]
}

// ConsumeFlags parses all flags up to the next non flag argument (argument does
// not start with "-" and isn't the value of a flag). It returns the remaining
// args.
func (b *ImportZipf) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("Import", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.Int64Var(&b.BaseBitmapID, "base-bitmap-id", 0, "")
	fs.Int64Var(&b.BaseProfileID, "base-profile-id", 0, "")
	fs.Int64Var(&b.MaxBitmapID, "max-bitmap-id", 1000, "")
	fs.Int64Var(&b.MaxProfileID, "max-profile-id", 1000, "")
	fs.IntVar(&b.Iterations, "iterations", 100000, "")
	fs.Int64Var(&b.Seed, "seed", 0, "")
	fs.StringVar(&b.Index, "index", "benchindex", "")
	fs.StringVar(&b.Frame, "frame", "frame", "")
	fs.IntVar(&b.BufferSize, "buffer-size", 10000000, "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
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
