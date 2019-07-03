package dx

import (
	"fmt"
	"os"
	"time"

	imagine "github.com/pilosa/tools/imagine/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// NewIngestCommand initializes a new ingest command for dx.
func NewIngestCommand() *cobra.Command {
	ingestCmd := &cobra.Command{
		Use:   "ingest",
		Short: "ingest randomly generated data",
		Long:  `Ingest randomly generated data from imagine tool in both instances of Pilosa.`,
		Run: func(cmd *cobra.Command, args []string) {

			if err := ExecuteIngest(); err != nil {
				fmt.Printf("%+v", err)
				os.Exit(1)
			}

		},
	}
	return ingestCmd
}

// ExecuteIngest executes the ingest on both Pilosa instances.
func ExecuteIngest() error {
	cResultChan := make(chan *Result, 1)
	pResultChan := make(chan *Result, 1)

	specsFiles := []string{m.SpecsFile}
	go runIngestOnInstance(newCandidateConfig(specsFiles), cResultChan)
	go runIngestOnInstance(newPrimaryConfig(specsFiles), pResultChan)

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

func newCandidateConfig(specsFiles []string) *imagine.Config {
	return newConfig(m.CHosts, m.CPort, specsFiles)
}

func newPrimaryConfig(specsFiles []string) *imagine.Config {
	return newConfig(m.PHosts, m.PPort, specsFiles)
}

func newConfig(hosts []string, port int, specsFiles []string) *imagine.Config {
	conf := &imagine.Config{
		Hosts:       hosts,
		Port:        port,
		Prefix:      m.Prefix,
		ThreadCount: m.ThreadCount,
	}
	conf.NewSpecsFiles(specsFiles)

	return conf
}
