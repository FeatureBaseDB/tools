package main

import (
	"context"
	"os"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

func NewRandomQueryCommand() *cobra.Command {
	b := bench.NewRandomQueryBenchmark()
	cmd := &cobra.Command{
		Use:   "random-query",
		Short: "Constructs and performs random queries.",
		Long: `Constructs and performs random queries.
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
	flags.IntVar(&b.MaxDepth, "max-depth", 4, "Maximum nesting of queries.")
	flags.IntVar(&b.MaxArgs, "max-args", 4, "Maximum number of arguments per query.")
	flags.IntVar(&b.MaxN, "max-n", 100, "Maximum value of N for TopN queries.")
	flags.Int64Var(&b.MinRowID, "min-row-id", 0, "Minimum row id to include in queries.")
	flags.Int64Var(&b.MaxRowID, "max-row-id", 100000, "Maximum row id to include in queries.")
	flags.Int64Var(&b.Seed, "seed", 1, "random seed")
	flags.IntVar(&b.Iterations, "iterations", 100, "Number queries to perform.")
	flags.StringVar(&b.Field, "field", defaultField, "Field to query.")
	flags.StringVar(&b.Index, "index", defaultIndex, "Pilosa index to use.")

	return cmd
}
