package dx

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pilosa/tools/imagine"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// NewSoloIngestCommand initializes a solo ingest command.
func NewSoloIngestCommand(m *Main) *cobra.Command {
	sIngestCmd := &cobra.Command{
		Use:   "ingest",
		Short: "ingest on a single cluster",
		Long:  `Perform ingest on a single cluster at a time.`,
		Run: func(cmd *cobra.Command, args []string) {

			if err := ExecuteSoloIngest(m); err != nil {
				fmt.Printf("%+v", err)
				os.Exit(1)
			}

		},
	}
	return sIngestCmd
}

// ExecuteSoloIngest executes the ingest on a single Pilosa instance.
func ExecuteSoloIngest(m *Main) error {
	specsFile := []string{m.SpecsFile}
	config := newConfig(m.Hosts, m.Port, specsFile, m.Prefix, m.ThreadCount)

	if m.SpecsFile == "" {
		m.SpecsFile = "specs.toml"
	}
	isFirstIngest, err := checkBenchIsFirst(m.SpecsFile, m.DataDir)
	if err != nil {
		return errors.Wrap(err, "error checking if prior bench exists")
	}

	if isFirstIngest {
		path := filepath.Join(m.DataDir, "specs.toml")
		if err = copyFile(path, m.SpecsFile); err != nil {
			return errors.Wrap(err, "error copying specs file to data directory")
		}
	}

	if m.Filename == "" {
		m.Filename = cmdIngest + "-" + time.Now().Format(time.RFC3339Nano)
	}

	return executeSoloIngest(config, m.Filename, m.DataDir)
}

func executeSoloIngest(config *imagine.Config, filename, dataDir string) error {
	bench, err := runSoloIngest(config)
	if err != nil {
		return errors.Wrap(err, "error running solo ingest")
	}

	if err = writeIngestResultFile(bench, filename, dataDir); err != nil {
		return errors.Wrap(err, "error writing result file")
	}
	return nil
}

func runSoloIngest(config *imagine.Config) (*SoloBenchmark, error) {
	resultChan := make(chan *Result, 1)
	go runIngestOnInstance(config, resultChan)

	result := <-resultChan
	if result.err != nil {
		return nil, errors.Wrap(result.err, "could not ingest")
	}

	bench := &SoloBenchmark {
		Type: cmdIngest,
		Time: TimeDuration{Duration: result.time},
	}
	return bench, nil
}

// writeResultFile writes the result of the solo ingest to the file.
func writeIngestResultFile(bench *SoloBenchmark, filename, dataDir string) error {
	if err := os.MkdirAll(dataDir, 0777); err != nil {
		return errors.Wrap(err, "error making data directory")
	}

	jsonBytes, err := json.Marshal(bench)
	if err != nil {
		return errors.Wrap(err, "could not marshal results to JSON")
	}

	path := filepath.Join(dataDir, filename)
	if err = ioutil.WriteFile(path, jsonBytes, 0666); err != nil {
		return errors.Wrap(err, "could not write JSON to file")
	}
	return nil
}

// copyFile copies a file src to dst, creating this if it doesn't exist.
func copyFile(dst, src string) error {
	file, err := os.Open(src)
	if err != nil {
		return errors.Wrap(err, "error opening src")
	}
	defer file.Close()

	newFile, err := os.Create(dst)
	if err != nil {
		return errors.Wrap(err, "error creating dst")
	}
	defer newFile.Close()
	if _, err := io.Copy(newFile, file); err != nil {
		return errors.Wrap(err, "error copying file")
	}
	return nil
}
