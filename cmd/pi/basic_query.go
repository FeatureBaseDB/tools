package main

import (
	"context"
	"os"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

func NewBasicQueryCommand() *cobra.Command {
	b := bench.NewBasicQueryBenchmark()
	cmd := &cobra.Command{
		Use:   "basic-query",
		Short: "Runs the given PQL query against pilosa multiple times with different arguments.",
		Long: `Runs the given PQL query against pilosa multiple times with different arguments.

Agent num has no effect.`,
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
	flags.IntVar(&b.Iterations, "iterations", 1, "Number of queries to make.")
	flags.IntVar(&b.NumArgs, "num-args", 2, "Number of rows to put in each query (i.e. number of rows to intersect)")
	flags.StringVar(&b.Query, "query", "Intersect", "query to perform (Intersect, Union, Difference, Xor)")
	flags.StringVar(&b.Field, "field", defaultField, "Field to query.")
	flags.StringVar(&b.Index, "index", defaultIndex, "Pilosa index to use.")

	return cmd
}
