package bench

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"context"
)

// Benchmark is an interface to guide the creation of new pilosa benchmarks or
// benchmark components. It defines 2 methods, Init, and Run. These are separate
// methods so that benchmark running code can time only the running of the
// benchmark, and not any setup.
type Benchmark interface {
	// Init takes a list of hosts and an agent number. It is generally expected
	// to set up a connection to pilosa using whatever client it chooses. The
	// agentNum should be used to parameterize the benchmark's configuration if
	// it is being run simultaneously on multiple "agents". E.G. the agentNum
	// might be used to make a random seed different for each agent, or have
	// each agent set a different set of bits. Init's doc string should document
	// how the agentNum affects it. Every benchmark should have a 'Name' field
	// set by init, which appears when the benchmark is marshalled to json as
	// "name".
	Init(hosts []string, agentNum int) error

	// Run runs the benchmark. The return value of Run is kept generic so that
	// any relevant statistics or metrics that may be specific to the benchmark
	// in question can be reported. TODO guidelines for what gets included in
	// results and what will get added by other stuff. Run does not need to
	// report total run time in `results`, as that will be added by calling
	// code.
	Run(ctx context.Context) map[string]interface{}
}

func getClusterVersion(ctx context.Context, hosts []string) (string, error) {
	if len(hosts) == 0 {
		return "", fmt.Errorf("can't get cluster version with no pilosa hosts configured")
	}
	resp, err := http.Get("http://" + hosts[0] + "/version")
	if err != nil {
		return "", fmt.Errorf("getClusterVersion http.Get: %v", err)
	}
	dec := json.NewDecoder(resp.Body)
	v := struct{ Version string }{}
	err = dec.Decode(&v)
	if err != nil {
		return "", fmt.Errorf("getClusterVersion decoding: %v", err)
	}
	return v.Version, nil
}

type BenchResult struct {
	Output        map[string]interface{} `json:"output"`
	Duration      time.Duration          `json:"duration"`
	AgentNum      int                    `json:"agentnum"`
	PilosaVersion string                 `json:"pilosa-version"`
	Configuration Benchmark              `json:"configuration"`
	Error         error                  `json:"error"`
}

func RunBenchmark(ctx context.Context, hosts []string, agentNum int, b Benchmark) *BenchResult {
	result := &BenchResult{}
	version, err := getClusterVersion(ctx, hosts)
	if err != nil {
		result.Error = err
		return result
	}

	err = b.Init(hosts, agentNum)
	if err != nil {
		result.Error = err
		return result
	}
	start := time.Now()
	result.Output = b.Run(context.Background())
	result.Duration = time.Since(start)
	result.AgentNum = agentNum
	result.PilosaVersion = version
	result.Configuration = b
	return result
}
