package dx

import (
	"fmt"
	"os"

	"github.com/pilosa/go-pilosa"
	"github.com/spf13/cobra"
	// "github.com/pilosa/tools/dx"
)

// TODO: add solo flag
// flags
var ThreadCount int
var CHosts []string
var PHosts []string
var CPort int
var PPort int
var SpecsFile = "./specs.toml"

func main() {
	if err := NewRootCmd().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// NewRootCmd creates an instance of the cobra root command for dx.
func NewRootCmd() *cobra.Command {
	rc := &cobra.Command{
		Use:   "dx",
		Short: "compare difference between two Pilosa instances",
		Long:  `Compare difference between candidate Pilosa instance and last known-good Pilosa version`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("dx tool")
		},
	}
	rc.PersistentFlags().IntVarP(&ThreadCount, "threadCount", "r", 1, "Number of concurrent goroutines to allocate")
	rc.PersistentFlags().StringSliceVar(&CHosts ,"cHosts", []string{"localhost"}, "Hosts of candidate instance")
	rc.PersistentFlags().StringSliceVar(&PHosts ,"pHosts", []string{"localhost"}, "Hosts of primary instance")
	rc.PersistentFlags().IntVar(&CPort, "cPort", 10101, "Port of candidate instance")
	// TODO: set default to 10101 for production
	rc.PersistentFlags().IntVar(&PPort, "pPort", 10102, "Port of primary instance")
	
	rc.AddCommand(NewIngestCommand())
	rc.AddCommand(NewQueryCommand())
	rc.AddCommand(NewAllCommand())

	rc.SetOutput(os.Stderr)

	return rc
}

func initializeClient(hosts []string, port int) (*pilosa.Client, error) {
	uris := make([]*pilosa.URI, 0, len(hosts))
	for _, host := range hosts {
		uri, err := pilosa.NewURIFromHostPort(host, uint16(port))
		if err != nil {
			return nil, fmt.Errorf("could not create Pilosa URI: %v", err)
		}
		uris = append(uris, uri)
	}

	cluster := pilosa.NewClusterWithHost(uris...)
	client, err := pilosa.NewClient(cluster)
	if err != nil {
		return nil, fmt.Errorf("could not create Pilosa cluster: %v", err)
	}
	return client, nil
} 