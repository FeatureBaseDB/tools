package dx

import (
	"fmt"
	"os"
	"time"

	"github.com/pilosa/tools/imagine"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// NewIngestCommand initializes a new ingest command for dx.
func NewIngestCommand(m *Main) *cobra.Command {
	ingestCmd := &cobra.Command{
		Use:   "ingest",
		Short: "ingest randomly generated data",
		Long:  `Ingest randomly generated data from imagine tool in both instances of Pilosa.`,
		Run: func(cmd *cobra.Command, args []string) {

			if err := ExecuteIngest(m); err != nil {
				fmt.Printf("%+v", err)
				os.Exit(1)
			}

		},
	}

	ingestCmd.PersistentFlags().StringVar(&m.SpecsFile, "specsfile", "", "Path to imagine specs file")

	return ingestCmd
}

// ExecuteIngest executes the ingest on both Pilosa instances.
func ExecuteIngest(m *Main) error {
	cResultChan := make(chan *Result, 1)
	pResultChan := make(chan *Result, 1)

	go runIngestOnInstance(newCandidateConfig(m), cResultChan)
	go runIngestOnInstance(newPrimaryConfig(m), pResultChan)

	cResult := <-cResultChan
	pResult := <-pResultChan

	if cResult.err != nil || pResult.err != nil {
		if cResult.err != nil {
			fmt.Printf("could not ingest in candidate: %v\n", cResult.err)
		}
		if pResult.err != nil {
			fmt.Printf("could not ingest in primary: %v\n", pResult.err)
		}
		return fmt.Errorf("error ingesting")
	}
	timeDelta := float64(cResult.time-pResult.time) / float64(pResult.time)

	b := &Benchmark{
		CTime:     cResult.time,
		PTime:     pResult.time,
		TimeDelta: timeDelta,
	}
	if err := printIngestResults(b); err != nil {
		return errors.Wrap(err, "could not print results")
	}
	return nil
}

// runIngestOnInstance ingests data based on a config file.
func runIngestOnInstance(conf *imagine.Config, resultChan chan *Result) {
	result := NewResult()

	err := conf.ReadSpecs()
	if err != nil {
		result.err = errors.Wrap(err, "config/spec error")
		resultChan <- result
		return
	}

	client, err := initializeClient(conf.Hosts, conf.Port)
	if err != nil {
		result.err = errors.Wrap(err, "could not create Pilosa client")
		resultChan <- result
		return
	}

	if err = conf.UpdateIndexes(client); err != nil {
		result.err = errors.Wrap(err, "could not update indexes")
		resultChan <- result
		return
	}

	now := time.Now()
	err = conf.ApplyWorkloads(client)
	if err != nil {
		result.err = errors.Wrap(err, "applying workloads")
		resultChan <- result
		return
	}

	result.time = time.Since(now)
	resultChan <- result
}

func newCandidateConfig(m *Main) *imagine.Config {
	specsFiles := []string{m.SpecsFile}
	return newConfig(m.CHosts, m.CPort, specsFiles, m.Prefix, m.ThreadCount)
}

func newPrimaryConfig(m *Main) *imagine.Config {
	specsFiles := []string{m.SpecsFile}
	return newConfig(m.PHosts, m.PPort, specsFiles, m.Prefix, m.ThreadCount)
}

func newConfig(hosts []string, port int, specsFiles []string, prefix string, threadCount int) *imagine.Config {
	conf := &imagine.Config{
		Hosts:       hosts,
		Port:        port,
		Prefix:      prefix,
		ThreadCount: threadCount,
	}
	conf.NewSpecsFiles(specsFiles)

	return conf
}
