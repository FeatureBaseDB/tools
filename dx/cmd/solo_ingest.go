package dx

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	imagine "github.com/pilosa/tools/imagine/pkg"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

// NewSoloIngestCommand initializes a solo ingest command.
func NewSoloIngestCommand() *cobra.Command {
	sIngestCmd := &cobra.Command{
		Use:   "ingest",
		Short: "ingest on a single cluster",
		Long:  `Perform ingest on a single cluster at a time.`,
		Run: func(cmd *cobra.Command, args []string) {

			if err := ExecuteSoloIngest(cmd.Flags()); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

		},
	}
	return sIngestCmd
}

// ExecuteSoloIngest executes the ingest on a single Pilosa instance.
func ExecuteSoloIngest(flags *flag.FlagSet) error {
	specsFiles := []string{m.SpecsFile}
	config, instanceType, err := newConfigFromFlags(flags, specsFiles)
	if err != nil {
		return err
	}
	otherInstanceType, err := otherInstance(instanceType)
	if err != nil {
		return fmt.Errorf("invalid instance type from flags: %v", err)
	}

	isFirstIngest, _, hashFilename, err := checkBenchIsFirst(m.SpecsFile, m.DataDir)
	if err != nil {
		return fmt.Errorf("error checking if prior bench exists: %v", err)
	}
	// append `ingest` to file name
	hashFilename = hashFilename + cmdIngest

	return executeSoloIngest(config, isFirstIngest, instanceType, otherInstanceType, hashFilename, m.DataDir)
}

func executeSoloIngest(config *imagine.Config, isFirstIngest bool, instanceType, otherInstanceType, filename, dataDir string) error {
	result, err := runSoloIngest(config)
	if err != nil {
		return fmt.Errorf("error running solo ingest: %v", err)
	}

	// This is the first ingest benchmark for the specs, so we save the result.
	if isFirstIngest {
		err := writeIngestResultFile(result, instanceType, filename, dataDir)
		return err
	}

	// A prior benchmark exists, so we read from that and compare the results.
	bench, err := readIngestResultFile(otherInstanceType, filename, dataDir)
	if err != nil {
		return fmt.Errorf("error reading result file: %v", err)
	}

	switch instanceType {
	case instanceCandidate:
		bench.CTime = result.time
	case instancePrimary:
		bench.PTime = result.time
	}

	bench.TimeDelta = float64(bench.CTime-bench.PTime) / float64(bench.PTime)
	if err = printIngestResults(bench); err != nil {
		return fmt.Errorf("could not print ingest comparison: %v", err)
	}

	// everything ran successfully, so we can safely delete the previous result file
	path := filepath.Join(dataDir, filename)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("everything ran successfully, but the previous result file could not be deleted: %v", err)
	}

	return nil
}

func runSoloIngest(config *imagine.Config) (*Result, error) {
	resultChan := make(chan *Result, 1)
	go runIngestOnInstance(config, resultChan)

	result := <-resultChan
	if result.err != nil {
		return nil, fmt.Errorf("could not ingest: %v", result.err)
	}

	return result, nil
}

// writeResultFile writes the result of the solo ingest to the file.
func writeIngestResultFile(result *Result, instanceType, filename, dataDir string) error {
	// TODO: make this path absolute
	if err := os.MkdirAll(dataDir, 0777); err != nil {
		return fmt.Errorf("error making data directory: %v", err)
	}
	file, err := os.Create(filepath.Join(dataDir, filename))
	if err != nil {
		return fmt.Errorf("error creating results file: %v", err)
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%v,%v,%v", cmdIngest, instanceType, result.time))
	if err != nil {
		return fmt.Errorf("could not write results file: %v", err)
	}
	// TODO: print single ingest result
	return file.Sync()
}

func readIngestResultFile(instanceType string, filename, dataDir string) (*Benchmark, error) {
	path := filepath.Join(dataDir, filename)

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open previous result file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	_ = scanner.Scan()
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning result file: %v", err)
	}

	text := scanner.Text()
	words := strings.Split(text, ",")
	if len(words) < 3 {
		return nil, fmt.Errorf("invalid number of values in result file")
	}
	if words[0] != cmdIngest {
		return nil, fmt.Errorf("results file should be tagged with ingest, got %v", words[0])
	}
	if words[1] != instanceType {
		return nil, fmt.Errorf("results file already has instance type %v", words[1])
	}
	duration, err := time.ParseDuration(words[2])
	if err != nil {
		return nil, fmt.Errorf("error parsing duration from results file: %v", err)
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

func newConfigFromFlags(flags *flag.FlagSet, specsFile []string) (*imagine.Config, string, error) {
	instanceType, err := determineInstance(flags)
	if err != nil {
		return nil, "", fmt.Errorf("could not create config: %v", err)
	}

	switch instanceType {
	case instanceCandidate:
		return newCandidateConfig(specsFile), instanceType, nil
	case instancePrimary:
		return newPrimaryConfig(specsFile), instanceType, nil
	}
	return nil, "", fmt.Errorf("unknown instance type: %v", instanceType)
}
