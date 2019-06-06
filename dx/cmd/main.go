package dx

import (
	"fmt"
	"os"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/spf13/cobra"
	// "github.com/pilosa/tools/dx"
)

// Main is the default type a dx command uses.
type Main struct {
	ThreadCount		int
	CHosts			[]string
	PHosts			[]string
	CPort			int
	PPort			int
	SpecsFile		string
	Solo			bool
}

// NewMain creates a new empty Main object.
func NewMain() *Main {
	return &Main{}
}

var m = NewMain()

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
		Short: "compare differences between two Pilosa instances",
		Long:  `Compare differences between candidate Pilosa instance and last known-good Pilosa version.`,
		Run: func(cmd *cobra.Command, args []string) {
			
		},
	}
	rc.PersistentFlags().IntVarP(&m.ThreadCount, "threadCount", "t", 1, "Number of concurrent goroutines to allocate")
	rc.PersistentFlags().StringSliceVar(&m.CHosts, "cHosts", []string{"localhost"}, "Hosts of candidate instance")
	rc.PersistentFlags().StringSliceVar(&m.PHosts, "pHosts", []string{"localhost"}, "Hosts of primary instance")
	rc.PersistentFlags().IntVar(&m.CPort, "cPort", 10101, "Port of candidate instance")
	// TODO: set default to 10101 for production
	rc.PersistentFlags().IntVar(&m.PPort, "pPort", 10102, "Port of primary instance")
	rc.PersistentFlags().StringVar(&m.SpecsFile, "specsFile", "./specs.toml", "Path to specs file")
	rc.PersistentFlags().BoolVarP(&m.Solo, "solo", "s", false, "Run on only one instace of Pilosa")

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

// Result is the result of running a single goroutine on
// a single Pilosa instance. Result will be the type passed
// around in channels while running multiple goroutines.
type Result struct {
	result pilosa.QueryResult
	time   time.Duration
	err    error
}

// NewResult initializes a new Result struct.
func NewResult() *Result {
	return &Result{
		result: nil,
		time:   0,
		err:    nil,
	}
}

type specsConfig struct {
	indexName	string
	fieldName	string
	min			int64
	max			int64
	columns		int64
}

// TODO: parse automatically
// based on specs.toml
var specs = specsConfig{
	indexName: 	"dx-index", 
	fieldName: 	"field", 
	min: 		0, 
	max: 		1000, 
	columns:	1000,
}

// TODO: BUFFER ALL CHANNELS
