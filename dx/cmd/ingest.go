package dx

import (
	"fmt"
	"sync"
	"time"

	imagine "github.com/pilosa/tools/imagine/pkg"
	"github.com/spf13/cobra"
)

// NewIngestCommand initializes a new ingest command for dx.
func NewIngestCommand() *cobra.Command {
	ingestCmd := &cobra.Command{
		Use:   "ingest",
		Short: "ingest randomly generated data",
		Long:  `Ingest randomly generated data from imagine tool in both instances of Pilosa`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Executing ingest")
			
			candidateCh := make(chan time.Duration, 1)
			primaryCh := make(chan time.Duration, 1)
			var wg sync.WaitGroup

			wg.Add(2)
			specsFiles := []string{SpecsFile}
			go timedIngest(newCandidateConfig(specsFiles), candidateCh, &wg)
			go timedIngest(newPrimaryConfig(specsFiles), primaryCh, &wg)

			wg.Wait()

			// TODO: multicolumn printing
			// TODO: add delta analysis
			fmt.Println(<-candidateCh)
			fmt.Println(<-primaryCh)

			fmt.Print("...done!")

		},
	}
	return ingestCmd
}


// TODO: error checking should quit program
// timedIngest executes an ingest command and sends the duration over the provided channel.
func timedIngest(conf *imagine.Config, timeChan chan time.Duration, wg *sync.WaitGroup) {
	now := time.Now()
	if err := executeIngest(conf); err != nil {
		fmt.Println("could not execute ingest: %v", err)
	}
	duration := time.Since(now)
	timeChan <- duration
	wg.Done()

}

// executeIngest ingests data based on a config file.
func executeIngest(conf *imagine.Config) error {
	err := conf.ReadSpecs()
	if err != nil {
		return fmt.Errorf("config/spec error: %v", err)
	}

	client, err := initializeClient(conf.Hosts, conf.Port)
	if err != nil {
		return fmt.Errorf("could not create Pilosa client: %v", err)
	}

	if err = conf.UpdateIndexes(client); err != nil {
		return fmt.Errorf("could not update indexes: %v", err)
	}
	
	// TODO: set stdout to null
	err = conf.ApplyWorkloads(client)
	if err != nil {
		return fmt.Errorf("applying workloads: %v", err)
	}

	return nil
}

func newCandidateConfig(specsFiles []string) *imagine.Config {
	return newConfig(CHosts, CPort, specsFiles)
}

func newPrimaryConfig(specsFiles []string) *imagine.Config {
	return newConfig(PHosts, PPort, specsFiles)
}

func newConfig(hosts []string, port int, specsFiles []string) *imagine.Config {
	conf := &imagine.Config{
		Hosts:			hosts,
		Port:			port,
		Prefix:			"dx-",
		ThreadCount:	ThreadCount,
	}
	conf.NewSpecsFiles(specsFiles)

	return conf
}
