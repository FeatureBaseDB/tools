package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewRandomQueryCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	randomQuery := &bench.RandomQuery{}
	randomQueryCmd := &cobra.Command{
		Use:   "random-query",
		Short: "Constructs and performs random queries.",
		Long: `Constructs and performs random queries.
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
			result := bench.RunBenchmark(context.Background(), hostSetup, agentNum, randomQuery)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := randomQueryCmd.Flags()
	flags.IntVar(&randomQuery.MaxDepth, "max-depth", 4, "Maximum nesting of queries.")
	flags.IntVar(&randomQuery.MaxArgs, "max-args", 4, "Maximum number of arguments per query.")
	flags.IntVar(&randomQuery.MaxN, "max-n", 100, "Maximum value of N for TopN queries.")

	flags.Int64Var(&randomQuery.MinRowID, "min-row-id", 0, "Minimum row id to include in queries.")
	flags.Int64Var(&randomQuery.MaxRowID, "max-row-id", 100000, "Maximum row id to include in queries.")
	flags.Int64Var(&randomQuery.Seed, "seed", 1, "random seed")
	flags.IntVar(&randomQuery.Iterations, "iterations", 100, "Number queries to perform.")
	flags.StringVar(&randomQuery.Frame, "frame", defaultFrame, "Frame to query.")
	flags.StringSliceVar(&randomQuery.Indexes, "indexes", []string{defaultIndex}, "Pilosa index to use.")
	flags.StringVar(&randomQuery.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'.")
	flags.StringVar(&randomQuery.ContentType, "content-type", "protobuf", "Can be protobuf or pql.")

	return randomQueryCmd
}

func init() {
	benchCommandFns["random-query"] = NewRandomQueryCommand
}
