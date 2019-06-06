package dx

import (
	"fmt"
	"os"
	"time"

	imagine "github.com/pilosa/tools/imagine/pkg"
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
				fmt.Println(err)
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

	cResult := <- cResultChan
	pResult := <- pResultChan

	if cResult.err != nil || pResult.err != nil {
		if cResult.err != nil {
			fmt.Printf("could not ingest in candidate: %v", cResult.err)
		}
		if pResult.err != nil {
			fmt.Printf("could not ingest in primary: %v", pResult.err)
		}
		return fmt.Errorf("error ingesting")
	}
	timeDelta := float64(cResult.time - pResult.time) / float64(pResult.time)

	printIngestResults(specs.columns, cResult.time, pResult.time, timeDelta)
	return nil
}


// runIngestOnInstance ingests data based on a config file.
func runIngestOnInstance(conf *imagine.Config, resultChan chan *Result) {
	result := NewResult()

	err := conf.ReadSpecs()
	if err != nil {
		result.err = fmt.Errorf("config/spec error: %v", err)
		resultChan <- result
		return
	}

	client, err := initializeClient(conf.Hosts, conf.Port)
	if err != nil {
		result.err = fmt.Errorf("could not create Pilosa client: %v", err)
		resultChan <- result
		return
	}

	now := time.Now()
	if err = conf.UpdateIndexes(client); err != nil {
		result.err = fmt.Errorf("could not update indexes: %v", err)
		resultChan <- result
		return
	}
	
	// TODO: set stdout to null
	err = conf.ApplyWorkloads(client)
	if err != nil {
		result.err = fmt.Errorf("applying workloads: %v", err)
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
		Hosts:			hosts,
		Port:			port,
		Prefix:			"dx-",
		ThreadCount:	m.ThreadCount,
	}
	conf.NewSpecsFiles(specsFiles)

	return conf
}