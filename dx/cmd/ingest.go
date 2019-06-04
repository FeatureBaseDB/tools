package dx

import (
	"fmt"
	"time"

	pilosa "github.com/pilosa/go-pilosa"
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

			go func() {
				now := time.Now()
				executeIngest(newCandidateConfig())
				duration := time.Since(now)
				fmt.Println(duration)
			}()

			go func() {
				now := time.Now()
				executeIngest(newCandidateConfig())
				duration := time.Since(now)
				fmt.Println(duration)
			}()

			fmt.Print("...done!")

		},
	}
	return ingestCmd
}

func executeIngest(conf *imagine.Config) {
	err := conf.ReadSpecs()
	if err != nil {
		fmt.Printf("config/spec error: %v", err)
	}

	uris := make([]*pilosa.URI, 0, len(conf.Hosts))
	for _, host := range conf.Hosts {
		uri, err := pilosa.NewURIFromHostPort(host, uint16(conf.Port))
		if err != nil {
			fmt.Printf("could not create Pilosa URI: %v", err)
		}
		uris = append(uris, uri)
	}

	cluster := pilosa.NewClusterWithHost(uris...)
	client, err := pilosa.NewClient(cluster)
	if err != nil {
		fmt.Printf("could not create Pilosa client: %v", err)
	}

	if err = conf.UpdateIndexes(client); err != nil {
		fmt.Printf("could not update indexes: %v", err)
	}
	
	err = conf.ApplyWorkloads(client)
	if err != nil {
		fmt.Printf("applying workloads: %v", err)
	}
}

func newCandidateConfig() *imagine.Config {
	return newConfig(CHosts, CPort)
}

func newPrimaryConfig() *imagine.Config {
	return newConfig(PHosts, PPort)
}

func newConfig(hosts []string, port int) *imagine.Config {
	conf := &imagine.Config{
		Hosts:			hosts,
		Port:			port,
		Prefix:			"dx-",
		ThreadCount:	ThreadCount,
	}
	specsFiles := []string{"./ingest.toml"}
	conf.NewSpecsFiles(specsFiles)

	return conf
}
