package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	pcli "github.com/pilosa/go-pilosa"
)

// Benchmark is an interface to guide the creation of new pilosa benchmarks or
// benchmark components. It defines 2 methods, Init, and Run. These are separate
// methods so that benchmark running code can time only the running of the
// benchmark, and not any setup. Benchmarks should Marshal to valid JSON so that
// their configuration may be recorded along with their results.
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

	// Run runs the benchmark. See the documentation for the Result object to
	// see how Run() should use its fields to return benchmark results.
	Run(ctx context.Context) *Result
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

// Results holds the output from the run of a benchmark - the Benchmark's Run()
// method may set Stats, Responses, and Extra, and the RunBenchmark helper
// function will set the Duration, AgentNum, PilosaVersion, and Configuration.
// Either may set Error if there is an error. The structure of Result assumes
// that most benchmarks will run multiple queries and track statistics about how
// long each one takes. The Extra field is for benchmarks which either do not
// fit this model, or want to return additional information not covered by Stats
// and Responses.
type Result struct {
	// Set By Benchmark.Run
	Stats     *Stats                 `json:"stats"`
	Responses []*pcli.QueryResponse  `json:"responses"`
	Extra     map[string]interface{} `json:"extra"`

	// Set By RunBenchmark
	Duration      time.Duration `json:"duration"`
	AgentNum      int           `json:"agentnum"`
	PilosaVersion string        `json:"pilosa-version"`
	Configuration interface{}   `json:"configuration"`

	err error

	// Error exists so that errors can be correctly marshalled to JSON. It is set using Result.err.Error()
	Error string `json:"error"`
}

// NewResult intializes and returns a Result.
func NewResult() *Result {
	return &Result{
		Stats:     NewStats(),
		Extra:     make(map[string]interface{}),
		Responses: make([]*pcli.QueryResponse, 0),
	}
}

// Add adds the duration to the Result's Stats object. If resp is non-nil, it
// also adds it to the slice of responses.
func (r *Result) Add(d time.Duration, resp *pcli.QueryResponse) {
	r.Stats.Add(d)
	if resp != nil {
		r.Responses = append(r.Responses, resp)
	}
}

// RunBenchmark invokes the given benchmark's Init and Run methods, and
// populates the Result object with their output, along with some metadata.
func RunBenchmark(ctx context.Context, hosts []string, agentNum int, b Benchmark) *Result {
	version, err := getClusterVersion(ctx, hosts)
	if err != nil {
		return &Result{err: err, Error: err.Error()}
	}

	err = b.Init(hosts, agentNum)
	if err != nil {
		return &Result{err: err, Error: err.Error()}
	}
	start := time.Now()
	result := b.Run(context.Background())
	result.Duration = time.Since(start)
	result.AgentNum = agentNum
	result.PilosaVersion = version
	result.Configuration = b
	if result.err != nil {
		result.Error = result.err.Error()
	}
	return result
}
