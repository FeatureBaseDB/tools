package dx

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Main contains the flags dx uses and a logger.
type Main struct {
	ThreadCount   int
	CHosts        []string
	PHosts        []string
	CPort         int
	PPort         int
	SpecsFile     string
	Verbose       bool
	Prefix        string
	NumQueries    []int
	NumRows       int64
	Filename      string
	DataDir       string
	ActualResults bool
	Hosts         []string
	Port          int
	QueryTemplate string
}

// NewMain creates a new empty Main object.
func NewMain() *Main {
	return &Main{
		Prefix: "dx-",
	}
}

// NewRootCmd creates an instance of the cobra root command for dx.
func NewRootCmd() *cobra.Command {
	// m is persisted to all subcommands
	m := NewMain()

	rc := &cobra.Command{
		Use:   "dx",
		Short: "compare differences between two Pilosa instances",
		Long:  `Compare differences between candidate Pilosa instance and last known-good Pilosa version.`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if m.Verbose {
				log.SetOutput(os.Stderr)
			} else {
				log.SetOutput(ioutil.Discard)
			}
		},
		Run: func(cmd *cobra.Command, args []string) {

			fmt.Println("dx is a tool used to measure the differences between two Pilosa instances. The following checks whether the two instances specified by the flags are running.")

			if err := printServers(m); err != nil {
				fmt.Printf("%+v", err)
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
	rc.PersistentFlags().BoolVarP(&m.ActualResults, "actualresults", "a", false, "Compare actual results of queries instead of counts")

	rc.AddCommand(NewIngestCommand(m))
	rc.AddCommand(NewQueryCommand(m))
	rc.AddCommand(NewSoloCommand(m))

	return rc
}

func printServers(m *Main) error {
	candidate, err := initializeClient(m.CHosts, m.CPort)
	if err != nil {
		return errors.Wrap(err, "could not create candidate client")
	}
	fmt.Printf("\nCandidate\non host/s %v and port %v\n", strings.Join(m.CHosts, ","), m.CPort)
	if err = printServerInfo(candidate); err != nil {
		return errors.Wrap(err, "could not print candidate server info")
	}

	primary, err := initializeClient(m.PHosts, m.PPort)
	if err != nil {
		return errors.Wrap(err, "could not create primary client")
	}
	fmt.Printf("Primary\non host/s %v and port %v\n", strings.Join(m.PHosts, ","), m.PPort)
	if err = printServerInfo(primary); err != nil {
		return errors.Wrap(err, "could not print primary server info: %v")
	}
	return nil
}

// modified from package imagine
func printServerInfo(client *pilosa.Client) error {
	serverInfo, err := client.Info()
	if err != nil {
		return errors.Wrap(err, "couldn't get server info")
	}
	serverMemMB := serverInfo.Memory / (1024 * 1024)
	serverMemGB := (serverMemMB + 1023) / 1024
	fmt.Printf("server memory: %dGB [%dMB]\n", serverMemGB, serverMemMB)
	fmt.Printf("server CPU: %s\n[%d physical cores, %d logical cores available]\n", serverInfo.CPUType, serverInfo.CPUPhysicalCores, serverInfo.CPULogicalCores)
	serverStatus, err := client.Status()
	if err != nil {
		return errors.Wrap(err, "couldn't get cluster status info")
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
			return nil, errors.Wrap(err, "could not create Pilosa URI")
		}
		uris = append(uris, uri)
	}

	cluster := pilosa.NewClusterWithHost(uris...)
	client, err := pilosa.NewClient(cluster)
	if err != nil {
		return nil, errors.Wrap(err, "could not create Pilosa cluster")
	}
	return client, nil
}

// Result is the result of running a single goroutine on
// a single Pilosa instance. Result will be the type passed
// around in channels while running multiple goroutines.
type Result struct {
	result      pilosa.QueryResult
	resultCount int64
	time        time.Duration
	err         error
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
