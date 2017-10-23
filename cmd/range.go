package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewRangeQueryCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	rangeQuery := &bench.RangeQuery{}
	rangeQueryCmd := &cobra.Command{
		Use:   "range-query",
		Short: "Constructs and performs range queries.",
		Long: `Constructs and performs range queries.
Agent num modifies random seed.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags := cmd.Flags()
			hostSetup, err := bench.HostSetupFromFlags(flags)
			if err != nil {
				return err
			}
			agentNum, err := flags.GetInt("agent-num")
			if err != nil {
				return err
			}
			result := bench.RunBenchmark(context.Background(), hostSetup, agentNum, rangeQuery)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := rangeQueryCmd.Flags()
	flags.IntVar(&rangeQuery.MaxDepth, "max-depth", 2, "Maximum nesting of queries.")
	flags.IntVar(&rangeQuery.MaxArgs, "max-args", 2, "Maximum number of arguments per query.")

	flags.Int64Var(&rangeQuery.MinRange, "min-range", 0, "Minimum range to include in queries.")
	flags.Int64Var(&rangeQuery.MaxRange, "max-range", 100, "Maximum range to include in queries.")
	flags.Int64Var(&rangeQuery.Seed, "seed", 1, "random seed")
	flags.IntVar(&rangeQuery.Iterations, "iterations", 100, "Number queries to perform.")
	flags.StringVar(&rangeQuery.Frame, "frame", defaultRangeFrame, "Frame to query.")
	flags.StringVar(&rangeQuery.Index, "indexes", defaultIndex, "Pilosa index to use.")
	flags.StringSliceVar(&rangeQuery.Fields, "fields", []string{defaultField}, "Pilosa fields to use.")
	flags.StringVar(&rangeQuery.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'.")
	flags.StringVar(&rangeQuery.ContentType, "content-type", "protobuf", "Can be protobuf or pql.")
	flags.StringVar(&rangeQuery.QueryType, "type", "sum", "Query type for range, default to sum")
	return rangeQueryCmd
}

func init() {
	benchCommandFns["range-query"] = NewRangeQueryCommand
}
