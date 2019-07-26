package dx

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const (
	cmdIngest = "ingest"
	cmdQuery  = "query"
	cmdTotal  = "total"
)

// Main contains the flags dx uses.
type Main struct {
	Hosts         []string
	ThreadCount   int
	SpecFiles    []string
	Verbose       bool
	Prefix        string
	NumQueries    int64
	NumRows       int64
	DataDir       string
	ActualResults bool
	QueryTemplate string
	Indexes       []string
}

// NewMain creates a new Main object.
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
		Short: "analyze accuracy and performance regression across Pilosa versions",
		Long:  `Analyze accuracy and performance regression across Pilosa versions by running high-load ingest and queries.`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// set logger
			if m.Verbose {
				log.SetOutput(os.Stderr)
			} else {
				log.SetOutput(ioutil.Discard)
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("dx is a tool used to analyze accuracy and performance regression across Pilosa versions.\nThe following checks whether the clusters specified by the hosts flag are running.\n\n")
			if err := printServers(m.Hosts); err != nil {
				if m.Verbose {
					fmt.Printf("%+v\n", err)
				} else {
					fmt.Printf("%v\n", err)
				}
				os.Exit(1)
			}
		},
	}

	// default
	var usrHomeDirDx string
	home, err := os.UserHomeDir()
	if err == nil {
		usrHomeDirDx = filepath.Join(home, "dx")
	}

	// TODO: flag for which folder to store run in?
	flags := rc.PersistentFlags()
	flags.StringArrayVarP(&m.Hosts, "hosts", "o", []string{"localhost:10101"}, "Comma-separated list of 'host:port' pairs. Repeat this flag for each cluster")
	flags.IntVarP(&m.ThreadCount, "threadcount", "t", 1, "Number of goroutines to allocate")
	flags.BoolVarP(&m.Verbose, "verbose", "v", false, "Enable verbose logging")
	flags.StringVarP(&m.DataDir, "datadir", "d", usrHomeDirDx, "Data directory to store results")

	rc.AddCommand(NewIngestCommand(m))
	rc.AddCommand(NewQueryCommand(m))
	rc.AddCommand(NewCompareCommand(m))

	return rc
}

func printServers(hosts []string) error {
	for _, clusterHostsString := range hosts {
		fmt.Printf("Cluster with hosts %v\n", clusterHostsString)
		clusterHosts := strings.Split(clusterHostsString, ",")

		client, err := initializeClient(clusterHosts...)
		if err != nil {
			fmt.Println(fmt.Errorf("error initializing client: %v", err))
		}
		if err = printServerInfo(client); err != nil {
			return errors.Wrap(err, "could not print server info")
		}
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

// initalizeClient creates the Pilosa clients from a slice of strings, where each string is
// a comma-separated list of host:port pairs in a cluster. The order in which the clusters
// appear in hosts is the same order as they appear in output slice.
func initializeClients(hosts []string) ([]*pilosa.Client, error) {
	// len(hosts) is the number of clusters
	clients := make([]*pilosa.Client, 0, len(hosts))
	for _, clusterHostsString := range hosts {
		clusterHosts := strings.Split(clusterHostsString, ",")

		client, err := initializeClient(clusterHosts...)
		if err != nil {
			return nil, errors.Wrap(err, "error creating client for cluster")
		}

		clients = append(clients, client)
	}
	return clients, nil
}

// initializeClient creates a Pilosa client using a list of hosts from the cluster.
func initializeClient(clusterHosts ...string) (*pilosa.Client, error) {
	// initialize uris from this cluster
	uris := make([]*pilosa.URI, 0, len(clusterHosts))
	for _, host := range clusterHosts {
		uri, err := pilosa.NewURIFromAddress(host)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating Pilosa URI from host %s", host)
		}
		uris = append(uris, uri)
	}

	// initialize cluster and client from the uris
	cluster := pilosa.NewClusterWithHost(uris...)
	client, err := pilosa.NewClient(cluster)
	if err != nil {
		return nil, errors.Wrap(err, "error creating Pilosa client from URIs")
	}

	return client, nil
}

// getAllClusterHosts creates a slice for each cluster containing the hosts in that cluster.
// Ex. ["host1,host2", "host3"] -> [[host1, host2], [host3]]
func getAllClusterHosts(hosts []string) [][]string {
	allClusterHosts := make([][]string, 0)
	for _, clusterHostsString := range hosts {
		clusterHosts := strings.Split(clusterHostsString, ",")
		allClusterHosts = append(allClusterHosts, clusterHosts)
	}
	return allClusterHosts
}

// makeFolder makes a folder with name "{cmd}-{time now}" in the directory.
// Ex. ("ingest", "usr/home") -> creates the directory usr/home/ingest-2019-07-10T15/52/44-05/00.
func makeFolder(cmdType, dir string) (string, error) {
	timestamp := time.Now().Format(time.RFC3339)
	folderName := cmdType + "-" + timestamp
	path := filepath.Join(dir, folderName)

	if err := os.MkdirAll(path, 0777); err != nil {
		return "", errors.Wrapf(err, "error mkdir for %v", path)
	}
	return path, nil
}

// TimeDuration wraps time.Duration to encode to JSON.
type TimeDuration struct {
	Duration time.Duration
}

// UnmarshalJSON deserializes json to TimeDuration.
func (d *TimeDuration) UnmarshalJSON(b []byte) (err error) {
	d.Duration, err = time.ParseDuration(strings.Trim(string(b), `"`))
	return
}

// MarshalJSON serializes TimeDuration to json.
func (d *TimeDuration) MarshalJSON() (b []byte, err error) {
	return []byte(fmt.Sprintf(`"%v"`, d.Duration)), nil
}

// Benchmark contains the information related to an ingest or query benchmark.
type Benchmark struct {
	Type        string       `json:"type"`
	Time        TimeDuration `json:"time"`
	ThreadCount int          `json:"threadcount"`
	Query       *Query       `json:"query,omitempty"`
}

// NewBenchmark creates an empty benchmark of type cmdType.
func NewBenchmark() *Benchmark {
	return &Benchmark{}
}
