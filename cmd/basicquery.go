package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewBasicQueryCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	basicQuery := &bench.BasicQuery{}
	basicQueryCmd := &cobra.Command{
		Use:   "basic-query",
		Short: "Runs the given PQL query against pilosa multiple times with different arguments.",
		Long: `Runs the given PQL query against pilosa multiple times with different arguments.

Agent num has no effect.`,
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
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, basicQuery)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := basicQueryCmd.Flags()
	flags.IntVar(&basicQuery.Iterations, "iterations", 1, "Number of queries to make.")
	flags.IntVar(&basicQuery.NumArgs, "num-args", 2, "Number of rows to put in each query (i.e. number of rows to intersect)")
	flags.StringVar(&basicQuery.Query, "query", "Intersect", "query to perform (Intersect, Union, Difference, Xor)")
	flags.StringVar(&basicQuery.Frame, "frame", "frame", "Frame to query.")
	flags.StringVar(&basicQuery.Index, "index", "benchindex", "Pilosa index to use.")
	flags.StringVar(&basicQuery.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'")
	flags.StringVar(&basicQuery.ContentType, "content-type", "protobuf", "Can be protobuf or pql.")

	return basicQueryCmd
}

func init() {
	benchCommandFns["basic-query"] = NewBasicQueryCommand
}
