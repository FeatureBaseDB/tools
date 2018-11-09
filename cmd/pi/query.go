package main

import (
	"context"
	"os"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewQueryCommand subcommands
func NewQueryCommand() *cobra.Command {
	b := bench.NewQueryBenchmark()
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Runs the given PQL query against pilosa and records the results along with the duration.",
		Long: `Runs the given PQL query against pilosa and records the results along with the duration.
Agent num has no effect`,
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
	flags.IntVar(&b.Iterations, "iterations", 1, "Number of times to repeat the query.")
	flags.StringVar(&b.Query, "query", "Count(Row(fbench=1))", "PQL query to perform.")
	flags.StringVar(&b.Index, "index", defaultIndex, "Pilosa index to use.")

	return cmd
}
