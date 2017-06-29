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
		Short: "Constructs random queries",
		Long:  `Agent num modifies random seed`,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags := cmd.Flags()
			hosts, err := flags.GetStringSlice("hosts")
			if err != nil {
				return err
			}
			agentNum, err := flags.GetInt("agent-num")
			if err != nil {
				return err
			}
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, randomQuery)
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
	flags.IntVar(&randomQuery.MaxN, "max-n", 4, "Maximum value of N for TopN queries.")

	flags.Int64Var(&randomQuery.BaseBitmapID, "base-bitmap-id", 0, "Lowest row id to include in queries.")
	flags.Int64Var(&randomQuery.BitmapIDRange, "bitmap-id-range", 100000, "Number of possible row ids to include in queries.")
	flags.Int64Var(&randomQuery.Seed, "seed", 1, "random seed")
	flags.IntVar(&randomQuery.Iterations, "iterations", 100, "number of bits to set")
	flags.StringVar(&randomQuery.Frame, "frame", "frame", "frame to query")
	flags.StringSliceVar(&randomQuery.Indexes, "indexes", []string{"benchindex"}, "pilosa index to use")
	flags.StringVar(&randomQuery.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'")
	flags.StringVar(&randomQuery.ContentType, "content-type", "protobuf", "protobuf or pql")

	return randomQueryCmd
}

func init() {
	benchCommandFns["random-query"] = NewRandomQueryCommand
}
