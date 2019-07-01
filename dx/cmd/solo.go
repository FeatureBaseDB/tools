package dx

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

const (
	instanceCandidate string = "candidate"
	instancePrimary   string = "primary"
	cmdIngest         string = "ingest"
	cmdQuery          string = "query"
)

func otherInstance(instanceType string) (string, error) {
	switch instanceType {
	case instanceCandidate:
		return instancePrimary, nil
	case instancePrimary:
		return instanceCandidate, nil
	}
	return "", fmt.Errorf("invalid instance type: %v", instanceType)
}

// NewSoloCommand initializes a solo command for dx. Functionally, `dx solo` does nothing meaningful
// except to print out server info, but the solo command is useful to signal to `dx` that the operation
// being ran is not happening concurrently on both machines.
func NewSoloCommand() *cobra.Command {
	soloCmd := &cobra.Command{
		Use:   "solo",
		Short: "dx on a single cluster",
		Long:  `Perform ingest or queries on a single cluster at a time.`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("running solo")
		},
	}
	soloCmd.PersistentFlags().StringVarP(&m.DataDir, "datadir", "d", "~/.dx/.solo", "Data directory to store results")

	soloCmd.AddCommand(NewSoloIngestCommand())
	soloCmd.AddCommand(NewSoloQueryCommand())

	return soloCmd
}

// determineInstance checks whether the solo command is being ran on candidate or primary.
func determineInstance(flags *flag.FlagSet) (string, error) {
	isCandidate := flags.Changed("chosts") || flags.Changed("cport")
	isPrimary := flags.Changed("phosts") || flags.Changed("pport")

	if isCandidate && isPrimary {
		return "", fmt.Errorf("cannot run dx solo on both candidate and primary at the same time")
	}
	if !isCandidate && !isPrimary {
		return "", fmt.Errorf("a flag must be set for at least one of chosts, cport, phosts, pport")
	}

	if isCandidate {
		return instanceCandidate, nil
	}
	return instancePrimary, nil
}

// TODO: get smarter about whether file exists but another type of dx command was used
// TODO: add threadCount to name

// checkBenchIsFirst checks whether the benchmark being ran using a specs file
// is the first by checking for the previous result in dataDir and returns
// isFirst, the hash string, and any error that might occur.
func checkBenchIsFirst(specsFile, dataDir string) (bool, bool, string, error) {
	hashFilename, err := hashSpecs(specsFile)
	if err != nil {
		return true, true, "", fmt.Errorf("error hashing file: %v", err)
	}

	ingestPath := filepath.Join(dataDir, hashFilename+cmdIngest)
	ingestFileExists, err := checkFileExists(ingestPath)
	if err != nil {
		return true, true, hashFilename, fmt.Errorf("error checking file existence: %v", err)
	}

	queryPath := filepath.Join(dataDir, hashFilename+cmdQuery+strconv.Itoa(m.ThreadCount))
	queryFileExists, err := checkFileExists(queryPath)
	if err != nil {
		return true, true, hashFilename, fmt.Errorf("error checking file existence: %v", err)
	}

	return !ingestFileExists, !queryFileExists, hashFilename, nil
}

// hashSpecs gets the sha256 hash of the specs file that will be the name of the results file.
func hashSpecs(specsFile string) (string, error) {
	file, err := os.Open(specsFile)
	if err != nil {
		return "", fmt.Errorf("could not open specs file: %v", err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("could not copy file to hash: %v", err)
	}

	hashBytes := hash.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)
	return hashString, nil
}

func checkFileExists(path string) (bool, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("error statting path: %v", err)
	}
	return true, nil
}
