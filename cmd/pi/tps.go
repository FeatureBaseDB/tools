package main

import (
	"context"
	"os"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewQueryCommand subcommands
func NewTPSCommand() *cobra.Command {
	b := bench.NewTPSBenchmark()
	com, err := cobrafy.Command(b)
	if err != nil {
		panic(err)
	}
	com.Use = b.Name
	com.Short = "Run TPS benchmark."
	com.Long = `Run TPS benchmark.

This benchmark spawns <concurrency> goroutines, each of which queries
Pilosa <iterations> times serially. The idea is to get an
understanding of what kind of query throughput various Pilosa
configurations can handle.

For this to be useful, you must already have an index in Pilosa with
at least 1 field which has some data in it. I recommend the "imagine"
tool (in this repository) for generating fake data with semi-realistic
characteristics.

For each query, TPS chooses randomly from the enabled query types
(intersect, union, difference, xor), and then chooses two random
fields, and two random rows within each field to perform the given
operation on. It wraps each query in a Count() to make the result size
consistent.

Currently, row IDs are always chosen randomly between min and max. If
no index is given, one is chosen at random, and if no fields are
given, all the fields in the index are used.

`

	com.RunE = func(cmd *cobra.Command, args []string) error {
		flags := cmd.Flags()
		b.Logger = NewLoggerFromFlags(flags)
		client, err := NewClientFromFlags(flags)
		if err != nil {
			return err
		}
		agentNum, err := flags.GetInt("agent-num")
		if err != nil {
			return err
		}
		result, err := b.Run(context.Background(), client, agentNum)
		if err != nil {
			result.Error = err.Error()
		}
		return PrintResults(cmd, result, os.Stdout)
	}
	return com
}
