package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewQueryCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	query := &bench.Query{}
	queryCmd := &cobra.Command{
		Use:   "query",
		Short: "Runs the given PQL query against pilosa and records the results along with the duration.",
		Long: `Runs the given PQL query against pilosa and records the results along with the duration.
Agent num has no effect`,
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
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, query)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := queryCmd.Flags()
	flags.IntVar(&query.Iterations, "iterations", 1, "Number of times to repeat the query.")
	flags.StringVar(&query.Query, "query", "Count(Bitmap(rowID=1, frame=fbench))", "PQL query to perform.")
	flags.StringVar(&query.Index, "index", defaultIndex, "Pilosa index to use.")
	flags.StringVar(&query.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'")
	flags.StringVar(&query.ContentType, "content-type", "protobuf", "Can be protobuf or pql.")

	return queryCmd
}

func init() {
	benchCommandFns["query"] = NewQueryCommand
}
