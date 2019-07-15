package dx

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pilosa/tools/imagine"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// NewIngestCommand initializes an ingest command.
func NewIngestCommand(m *Main) *cobra.Command {
	ingestCmd := &cobra.Command{
		Use:   "ingest",
		Short: "ingest on cluster/s using imagine",
		Long:  `Perform ingest the cluster/s using imagine.`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := ExecuteIngest(m); err != nil {
				fmt.Printf("%+v", err)
				os.Exit(1)
			}
		},
	}

	flags := ingestCmd.PersistentFlags()
	flags.StringSliceVar(&m.SpecsFiles, "specsfiles", nil, "Path to imagine specs file")
	ingestCmd.MarkFlagRequired("specsfile")

	return ingestCmd
}

// ExecuteIngest executes an ingest command on the cluster/s, ensuring that the order of clusters
// specified in the flags corresponds to the filenames that the results are saved in.
func ExecuteIngest(m *Main) error {
	for _, file := range m.SpecsFiles {
		found, err := checkFileExists(file)
		if err != nil {
			return errors.Wrapf(err, "error checking existence of %v", file)
		}
		if !found {
			return errors.Errorf("%s does not exist", file)
		}
	}

	path, err := makeFolder(cmdIngest, m.DataDir)
	if err != nil {
		return errors.Wrap(err, "error creating folder for ingest results")
	}

	// TODO: copy specs file?
	configs := make([]*imagine.Config, 0)

	allClusterHosts := getAllClusterHosts(m.Hosts)
	for _, clusterHosts := range allClusterHosts {
		config := newConfig(clusterHosts, m.SpecsFiles, m.Prefix, m.ThreadCount)
		configs = append(configs, config)
	}

	var wg sync.WaitGroup

	for i, config := range configs {
		wg.Add(1)
		go func(i int, config *imagine.Config) {
			defer wg.Done()
			ingestAndWriteResult(i, config, path)
		}(i, config)
	}

	wg.Wait()
	return nil
}

func ingestAndWriteResult(instanceNum int, config *imagine.Config, path string) {
	bench, err := ingestOnInstance(config)
	if err != nil {
		log.Printf("error ingesting on instance %v: %+v", instanceNum, err)
	}

	filename := strconv.Itoa(instanceNum)
	if err := writeResultFile(bench, filename, path); err != nil {
		log.Printf("error writing result file: %v", err)
	}
}

// runIngestOnInstance ingests data based on a config file.
func ingestOnInstance(conf *imagine.Config) (*Benchmark, error) {
	bench := NewBenchmark()
	bench.Type = cmdIngest
	bench.ThreadCount = conf.ThreadCount

	err := conf.ReadSpecs()
	if err != nil {
		return nil, errors.Wrap(err, "error reading specs from config")
	}

	client, err := initializeClient(conf.Hosts...)
	if err != nil {
		return nil, errors.Wrap(err, "error creating Pilosa client")
	}

	if err = conf.UpdateIndexes(client); err != nil {
		return nil, errors.Wrap(err, "error updating indexes")
	}

	now := time.Now()
	err = conf.ApplyWorkloads(client)
	if err != nil {
		return nil, errors.Wrap(err, "error applying workloads")
	}

	bench.Time.Duration = time.Since(now)
	return bench, nil
}

// writeResultFile writes the results of a SoloBenchmark to a JSON file.
func writeResultFile(bench *Benchmark, filename, dir string) error {
	jsonBytes, err := json.Marshal(bench)
	if err != nil {
		return errors.Wrap(err, "could not marshal results to JSON")
	}

	path := filepath.Join(dir, filename)
	if err = ioutil.WriteFile(path, jsonBytes, 0666); err != nil {
		return errors.Wrap(err, "could not write JSON to file")
	}
	return nil
}

func newConfig(hosts []string, specsFiles []string, prefix string, threadCount int) *imagine.Config {
	conf := &imagine.Config{
		Hosts:       hosts,
		Prefix:      prefix,
		ThreadCount: threadCount,
	}
	conf.NewSpecsFiles(specsFiles)

	return conf
}
