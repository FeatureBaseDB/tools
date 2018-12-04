package main

import (
	"context"
	"os"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

func NewRangeQueryCommand() *cobra.Command {
	b := bench.NewRangeQueryBenchmark()
	cmd := &cobra.Command{
		Use:   "range-query",
		Short: "Constructs and performs range queries.",
		Long: `Constructs and performs range queries.
Agent num modifies random seed.`,
		RunE: func(cmd *cobra.Command, args []string) error {
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
		},
	}

	flags := cmd.Flags()
	flags.IntVar(&b.MaxDepth, "max-depth", 2, "Maximum nesting of queries.")
	flags.IntVar(&b.MaxArgs, "max-args", 2, "Maximum number of arguments per query.")
	flags.Int64Var(&b.MinRange, "min-range", 0, "Minimum range to include in queries.")
	flags.Int64Var(&b.MaxRange, "max-range", 100, "Maximum range to include in queries.")
	flags.Int64Var(&b.Seed, "seed", 1, "random seed")
	flags.IntVar(&b.Iterations, "iterations", 100, "Number queries to perform.")
	flags.StringVar(&b.Field, "field", defaultField, "Field to query.")
	flags.StringVar(&b.Index, "index", defaultIndex, "Pilosa index to use.")
	flags.StringVar(&b.QueryType, "type", "sum", "Query type for range, default to sum")
	return cmd
}
