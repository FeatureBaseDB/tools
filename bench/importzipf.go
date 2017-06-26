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
	Name            string  `json:"name"`
	BaseBitmapID    int64   `json:"base-bitmap-id"`
	BaseProfileID   int64   `json:"base-profile-id"`
	MaxBitmapID     int64   `json:"max-bitmap-id"`
	MaxProfileID    int64   `json:"max-profile-id"`
	Iterations      int     `json:"iterations"`
	Random          bool    `json:"random"`
	Seed            int64   `json:"seed"`
	BitmapExponent  float64 `json:"bitmap-exponent"`
	BitmapRatio     float64 `json:"bitmap-ratio"`
	ProfileExponent float64 `json:"profile-exponent"`
	ProfileRatio    float64 `json:"profile-ratio"`
	bitmapRng       *rand.Zipf
	profileRng      *rand.Zipf
	bitmapPerm      *PermutationGenerator
	profilePerm     *PermutationGenerator
	importStdin     io.Writer

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

	-random
		if this option is set, profile and bitmap ids will be permuted, so the
		zipf distribution will only be apparent when they are sorted by count.

	-seed int
		seed for RNG

	-index string
		pilosa index to use

	-frame string
		frame to import into

	-bitmap-exponent float64
		zipf exponent parameter for bitmap IDs

	-bitmap-ratio float64
		zipf probability ratio parameter for bitmap IDs

	-profile-exponent float64
		zipf exponent parameter for profile IDs

	-profile-ratio float64
		zipf probability ratio parameter for profile IDs

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
	fs.BoolVar(&b.Random, "random", true, "")
	fs.Int64Var(&b.Seed, "seed", 0, "")
	fs.StringVar(&b.Index, "index", "benchindex", "")
	fs.StringVar(&b.Frame, "frame", "frame", "")
	fs.Float64Var(&b.BitmapExponent, "bitmap-exponent", 1.01, "")
	fs.Float64Var(&b.BitmapRatio, "bitmap-ratio", 0.25, "")
	fs.Float64Var(&b.ProfileExponent, "profile-exponent", 1.01, "")
	fs.Float64Var(&b.ProfileRatio, "profile-ratio", 0.25, "")
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
	rnd := rand.New(rand.NewSource(b.Seed))
	bitmapRange := b.MaxBitmapID - b.BaseBitmapID + 1
	profileRange := b.MaxProfileID - b.BaseProfileID
	bitmapOffset := getZipfOffset(bitmapRange+1, b.BitmapExponent, b.BitmapRatio)
	b.bitmapRng = rand.NewZipf(rnd, b.BitmapExponent, bitmapOffset, uint64(bitmapRange))
	profileOffset := getZipfOffset(profileRange+1, b.ProfileExponent, b.ProfileRatio)
	b.profileRng = rand.NewZipf(rnd, b.ProfileExponent, profileOffset, uint64(profileRange))

	b.bitmapPerm = NewPermutationGenerator(bitmapRange, b.Seed)
	b.profilePerm = NewPermutationGenerator(profileRange, b.Seed+1)

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
			done <- err
		}
		close(done)
	}()
	err := b.GenerateImportZipfCSV(b.importStdin)
	err = <-done
	if err != nil {
		results["error"] = err.Error()
	}
	return results
}

// GenerateImportCSV writes a generated csv to 'w' which is in the form pilosa/ctl expects for imports.
func (b *ImportZipf) GenerateImportZipfCSV(w io.Writer) error {
	for n := 0; n < b.Iterations; n++ {
		// generate IDs from Zipf distribution
		bitmapID := b.bitmapRng.Uint64()
		profID := b.profileRng.Uint64()
		// permute IDs randomly, but repeatably
		if b.Random {
			bitmapID = uint64(b.bitmapPerm.Next(int64(bitmapID)))
			profID = uint64(b.profilePerm.Next(int64(profID)))
		}
		_, err := w.Write([]byte(fmt.Sprintf("%d,%d\n", bitmapID, profID)))
		if err != nil {
			return fmt.Errorf("GenerateImportZipfCSV, writing: %v", err)
		}
	}
	return nil
}
