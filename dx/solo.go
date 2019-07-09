package dx

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const (
	cmdIngest string = "ingest"
	cmdQuery  string = "query"
)

// NewSoloCommand initializes a solo command for dx. Functionally, `dx solo` does nothing meaningful, 
// but the solo command is useful to signal to `dx` that the operation
// being ran is not happening concurrently on both machines.
func NewSoloCommand(m *Main) *cobra.Command {
	soloCmd := &cobra.Command{
		Use:   "solo",
		Short: "dx on a single cluster",
		Long:  `Perform ingest or queries on a single cluster at a time.`,
	}

	var path string
	home, err := os.UserHomeDir()
	if err == nil {
		path = filepath.Join(home, "dx", "solo")
	}

	soloCmd.PersistentFlags().StringVarP(&m.Filename, "filename", "f", "", "Name of file to save result in")
	soloCmd.PersistentFlags().StringVarP(&m.DataDir, "datadir", "d", path, "Data directory to store results")
	soloCmd.PersistentFlags().StringSliceVar(&m.Hosts, "hosts", []string{"localhost"}, "Hosts of Pilosa cluster")
	soloCmd.PersistentFlags().IntVar(&m.Port, "port", 10101, "Port of Pilosa cluster")

	soloCmd.AddCommand(NewSoloIngestCommand(m))
	soloCmd.AddCommand(NewSoloQueryCommand(m))
	soloCmd.AddCommand(NewSoloCompareCommand(m))

	return soloCmd
}

// checkBenchIsFirst checks whether the benchmark being ran is the first by checking
// whether a filename in dataDir exists, which would mean that a prior bench exists.
func checkBenchIsFirst(filename, dataDir string) (bool, error) {
	path := filepath.Join(dataDir, filename)
	fileExists, err := checkFileExists(path)
	if err != nil {
		return true, errors.Wrapf(err, "could not check if file %v exists", filename)
	}
	return !fileExists, nil
}

func checkFileExists(path string) (bool, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, errors.Wrap(err, "error statting path")
	}
	return true, nil
}

// TimeDuration wraps time.Duration to encode to JSON.
type TimeDuration struct {
	Duration time.Duration
}

// UnmarshalJSON to satisfy
func (d *TimeDuration) UnmarshalJSON(b []byte) (err error) {
	d.Duration, err = time.ParseDuration(strings.Trim(string(b), `"`))
	return
}

// MarshalJSON to satisfy
func (d *TimeDuration) MarshalJSON() (b []byte, err error) {
	return []byte(fmt.Sprintf(`"%v"`, d.Duration)), nil
}
