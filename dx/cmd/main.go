package dx

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/spf13/cobra"
)

func main() {
	if err := NewRootCmd().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func newLogger(verbose bool) *log.Logger {
	if verbose {
		return log.New(os.Stderr, "", log.LstdFlags)
	}
	return log.New(ioutil.Discard, "", log.LstdFlags)
}

// Main contains the flags dx uses and a logger.
type Main struct {
	ThreadCount int
	CHosts      []string
	PHosts      []string
	CPort       int
	PPort       int
	SpecsFile   string
	Verbose     bool
	Prefix      string
	NumQueries  []int	// slice of numbers of queries to run
	NumRows     int64	// number of rows to intersect in a query
	DataDir		string	// data directory to store results for solo command
	Logger      *log.Logger
}

// NewMain creates a new empty Main object.
func NewMain() *Main {
	return &Main{
		Prefix: "dx-",
	}
}

// m is the only global variable and contains necessary flags and the logger.
// The default values for the fields of m are set through cobra NewRootCmd().
var m = NewMain()

// NewRootCmd creates an instance of the cobra root command for dx.
func NewRootCmd() *cobra.Command {
	rc := &cobra.Command{
		Use:   "dx",
		Short: "compare differences between two Pilosa instances",
		Long:  `Compare differences between candidate Pilosa instance and last known-good Pilosa version.`,
		Run: func(cmd *cobra.Command, args []string) {

			fmt.Println("dx is a tool used to measure the differences between two Pilosa instances. The following checks whether the two instances specified by the flags are running.")

			if err := printServers(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	rc.PersistentFlags().IntVarP(&m.ThreadCount, "threadcount", "t", 1, "Number of goroutines to allocate")
	rc.PersistentFlags().StringSliceVar(&m.CHosts, "chosts", []string{"localhost"}, "Hosts of candidate instance")
	rc.PersistentFlags().StringSliceVar(&m.PHosts, "phosts", []string{"localhost"}, "Hosts of primary instance")
	rc.PersistentFlags().IntVar(&m.CPort, "cport", 10101, "Port of candidate instance")
	rc.PersistentFlags().IntVar(&m.PPort, "pport", 10101, "Port of primary instance")
	rc.PersistentFlags().StringVar(&m.SpecsFile, "specsfile", "", "Path to specs file")
	rc.PersistentFlags().StringVarP(&m.Prefix, "prefix", "p", "dx-", "Prefix to use for index")
	rc.PersistentFlags().BoolVarP(&m.Verbose, "verbose", "v", false, "Enable verbose logging")

	rc.MarkPersistentFlagRequired("specsfile")

	m.Logger = newLogger(m.Verbose)

	rc.AddCommand(NewIngestCommand())
	rc.AddCommand(NewQueryCommand())
	rc.AddCommand(NewSoloCommand())

	rc.SetOutput(os.Stderr)

	return rc
}

func printServers() error {
	candidate, err := initializeClient(m.CHosts, m.CPort)
	if err != nil {
		return fmt.Errorf("could not create candidate client: %v", err)
	}
	fmt.Printf("\nCandidate\non host/s %v and port %v\n", strings.Join(m.CHosts, ","), m.CPort)
	if err = printServerInfo(candidate); err != nil {
		return fmt.Errorf("could not print candidate server info: %v", err)
	}

	primary, err := initializeClient(m.PHosts, m.PPort)
	if err != nil {
		return fmt.Errorf("could not create primary client: %v", err)
	}
	fmt.Printf("Primary\non host/s %v and port %v\n", strings.Join(m.PHosts, ","), m.PPort)
	if err = printServerInfo(primary); err != nil {
		return fmt.Errorf("could not print primary server info: %v", err)
	}
	return nil
}

// modified from package imagine
func printServerInfo(client *pilosa.Client) error {
	serverInfo, err := client.Info()
	if err != nil {
		return fmt.Errorf("couldn't get server info: %v", err)
	}
	serverMemMB := serverInfo.Memory / (1024 * 1024)
	serverMemGB := (serverMemMB + 1023) / 1024
	fmt.Printf("server memory: %dGB [%dMB]\n", serverMemGB, serverMemMB)
	fmt.Printf("server CPU: %s\n[%d physical cores, %d logical cores available]\n", serverInfo.CPUType, serverInfo.CPUPhysicalCores, serverInfo.CPULogicalCores)
	serverStatus, err := client.Status()
	if err != nil {
		return fmt.Errorf("couldn't get cluster status info: %v", err)
	}
	fmt.Printf("cluster nodes: %d\n\n", len(serverStatus.Nodes))
	return nil
}

// initalizeClient creates a Pilosa cluster from a list of hosts and a common port.
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
	return &Result{}
}

// Benchmark is the result of executing an ingest or query benchmark.
// This is the result passed on to printing.
type Benchmark struct {
	Size      int64
	Accuracy  float64       // only for queries
	CTime     time.Duration // candidate total time
	PTime     time.Duration // primary total time
	TimeDelta float64
}
