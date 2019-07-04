package dx

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pilosa/tools/imagine"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

// NewSoloIngestCommand initializes a solo ingest command.
func NewSoloIngestCommand(m *Main) *cobra.Command {
	sIngestCmd := &cobra.Command{
		Use:   "ingest",
		Short: "ingest on a single cluster",
		Long:  `Perform ingest on a single cluster at a time.`,
		Run: func(cmd *cobra.Command, args []string) {

			if err := ExecuteSoloIngest(m, cmd.Flags()); err != nil {
				fmt.Printf("%+v", err)
				os.Exit(1)
			}

		},
	}
	return sIngestCmd
}

// ExecuteSoloIngest executes the ingest on a single Pilosa instance.
func ExecuteSoloIngest(m *Main, flags *flag.FlagSet) error {
	config, instanceType, err := newConfigFromFlags(m, flags)
	if err != nil {
		return err
	}
	otherInstanceType, err := otherInstance(instanceType)
	if err != nil {
		return errors.Wrap(err, "invalid instance type from flags")
	}

	isFirstIngest, _, hashFilename, err := checkBenchIsFirst(m.SpecsFile, m.DataDir)
	if err != nil {
		return errors.Wrap(err, "error checking if prior bench exists")
	}
	// append `ingest` to file name
	hashFilename = hashFilename + cmdIngest

	return executeSoloIngest(config, isFirstIngest, instanceType, otherInstanceType, hashFilename, m.DataDir)
}

func executeSoloIngest(config *imagine.Config, isFirstIngest bool, instanceType, otherInstanceType, filename, dataDir string) error {
	result, err := runSoloIngest(config)
	if err != nil {
		return errors.Wrap(err, "error running solo ingest")
	}

	// This is the first ingest benchmark for the specs, so we save the result.
	if isFirstIngest {
		err := writeIngestResultFile(result, instanceType, filename, dataDir)
		return err
	}

	// A prior benchmark exists, so we read from that and compare the results.
	bench, err := readIngestResultFile(otherInstanceType, filename, dataDir)
	if err != nil {
		return errors.Wrap(err, "error reading result file")
	}

	switch instanceType {
	case instanceCandidate:
		bench.CTime = result.time
	case instancePrimary:
		bench.PTime = result.time
	}

	bench.TimeDelta = float64(bench.CTime-bench.PTime) / float64(bench.PTime)
	if err = printIngestResults(bench); err != nil {
		return errors.Wrap(err, "could not print ingest comparison")
	}

	// everything ran successfully, so we can safely delete the previous result file
	path := filepath.Join(dataDir, filename)
	if err := os.Remove(path); err != nil {
		return errors.Wrap(err, "everything ran successfully, but the previous result file could not be deleted")
	}

	return nil
}

func runSoloIngest(config *imagine.Config) (*Result, error) {
	resultChan := make(chan *Result, 1)
	go runIngestOnInstance(config, resultChan)

	result := <-resultChan
	if result.err != nil {
		return nil, errors.Wrap(result.err, "could not ingest")
	}

	return result, nil
}

// writeResultFile writes the result of the solo ingest to the file.
func writeIngestResultFile(result *Result, instanceType, filename, dataDir string) error {
	// TODO: make this path absolute
	if err := os.MkdirAll(dataDir, 0777); err != nil {
		return errors.Wrap(err, "error making data directory")
	}
	file, err := os.Create(filepath.Join(dataDir, filename))
	if err != nil {
		return errors.Wrap(err, "error creating results file")
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%v,%v,%v", cmdIngest, instanceType, result.time))
	if err != nil {
		return errors.Wrap(err, "could not write results file")
	}
	// TODO: print single ingest result
	return file.Sync()
}

func readIngestResultFile(instanceType string, filename, dataDir string) (*Benchmark, error) {
	path := filepath.Join(dataDir, filename)

	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "could not open previous result file")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	_ = scanner.Scan()
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "error scanning result file")
	}

	text := scanner.Text()
	words := strings.Split(text, ",")
	if len(words) < 3 {
		return nil, errors.New("invalid number of values in result file")
	}
	if words[0] != cmdIngest {
		return nil, errors.Errorf("results file should be tagged with ingest, got %v", words[0])
	}
	if words[1] != instanceType {
		return nil, errors.Errorf("results file already has instance type %v", words[1])
	}
	duration, err := time.ParseDuration(words[2])
	if err != nil {
		return nil, errors.Wrap(err, "error parsing duration from results file")
	}

	bench := &Benchmark{}
	switch instanceType {
	case instanceCandidate:
		bench.CTime = duration
	case instancePrimary:
		bench.PTime = duration
	}

	return bench, nil
}

func newConfigFromFlags(m *Main, flags *flag.FlagSet) (*imagine.Config, string, error) {
	instanceType, err := determineInstance(flags)
	if err != nil {
		return nil, "", errors.Wrap(err, "could not create config")
	}

	switch instanceType {
	case instanceCandidate:
		return newCandidateConfig(m), instanceType, nil
	case instancePrimary:
		return newPrimaryConfig(m), instanceType, nil
	}
	return nil, "", errors.Errorf("unknown instance type: %v", instanceType)
}
