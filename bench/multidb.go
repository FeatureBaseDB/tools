package bench

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"
)

// MultiIndexSetBits sets bits with increasing profile id and bitmap id.
type MultiIndexSetBits struct {
	HasClient
	Name          string `json:"name"`
	BaseBitmapID  int    `json:"base-bitmap-id"`
	BaseProfileID int    `json:"base-profile-id"`
	Iterations    int    `json:"iterations"`
	Index         string `json:"index"`
	Frame         string `json:"frame"`
}

// Init sets up the index name based on the agentNum and sets up the pilosa client.
func (b *MultiIndexSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "multi-index-set-bits"
	b.Index = b.Index + strconv.Itoa(agentNum)
	err := initIndex(hosts[0], b.Index, b.Frame)
	if err != nil {
		return err
	}
	return b.HasClient.Init(hosts, agentNum)
}

// Usage returns the usage message to be printed.
func (b *MultiIndexSetBits) Usage() string {
	return `
multi-index-set-bits sets bits with increasing profile id and bitmap id using a different Index for each agent.

Agent num changes the index being written to.

Usage: multi-index-set-bits [arguments]

The following arguments are available:

	-base-bitmap-id int
		bits being set will all be greater than base-bitmap-id

	-base-profile-id int
		profile id num to start from

	-iterations int
		number of bits to set

	-client-type string
		Can be 'single' (all agents hitting one host) or 'round_robin'

	-content-type string
		protobuf or pql

`[1:]
}

// ConsumeFlags parses all flags up to the next non flag argument (argument does
// not start with "-" and isn't the value of a flag). It returns the remaining
// args.
func (b *MultiIndexSetBits) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("MultiIndexSetBits", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&b.BaseBitmapID, "base-bitmap-id", 0, "")
	fs.IntVar(&b.BaseProfileID, "base-profile-id", 0, "")
	fs.IntVar(&b.Iterations, "iterations", 100, "")
	fs.StringVar(&b.Index, "index", "benchindex", "")
	fs.StringVar(&b.Frame, "frame", "multi-index-set-bits", "")
	fs.StringVar(&b.ClientType, "client-type", "single", "")
	fs.StringVar(&b.ContentType, "content-type", "protobuf", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Run runs the MultiIndexSetBits benchmark
func (b *MultiIndexSetBits) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	if b.client == nil {
		results["error"] = fmt.Errorf("No client set for MultiIndexSetBits")
		return results
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		query := fmt.Sprintf("SetBit(frame='%s', rowID=%d, columnID=%d)", b.Frame, b.BaseBitmapID+n, b.BaseProfileID+n)
		start = time.Now()
		_, err := b.client.ExecuteQuery(ctx, b.Index, query, true)
		if err != nil {
			results["error"] = err
			return results
		}
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
