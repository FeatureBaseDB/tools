package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewMultipleSetBitsCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	multipleSetBit := &bench.MultipleSetBits{}
	multipleSetBitsCmd := &cobra.Command{
		Use:   "multiple-setbit",
		Short: "Sets multiple bits.",
		Long: `Sets multiple bits according to the parameters.
Agent num modifies random seed`,
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
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, multipleSetBit)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := multipleSetBitsCmd.Flags()
	flags.Int64Var(&multipleSetBit.BaseRowID, "base-row-id", 0, "Minimum row id for bits to be set.")
	flags.Int64Var(&multipleSetBit.RowIDRange, "row-id-range", 100000, "Number of rows in which to set bits.")
	flags.Int64Var(&multipleSetBit.BaseColumnID, "base-column-id", 0, "Minimum column id for bits to be set.")
	flags.Int64Var(&multipleSetBit.ColumnIDRange, "column-id-range", 100000, "Number of columsn in which to set bits.")
	flags.IntVar(&multipleSetBit.BatchSize, "batch-size", 5000, "Number of set bits per request")
	flags.Int64Var(&multipleSetBit.Seed, "seed", 1, "Random seed.")
	flags.IntVar(&multipleSetBit.Iterations, "iterations", 100, "Number of bits to set.")
	flags.StringVar(&multipleSetBit.Frame, "frame", defaultFrame, "Frame to set bits in.")
	flags.StringVar(&multipleSetBit.Index, "index", defaultIndex, "Pilosa index to use.")
	flags.StringVar(&multipleSetBit.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'")
	flags.StringVar(&multipleSetBit.ContentType, "content-type", "protobuf", "Can be protobuf or pql.")

	return multipleSetBitsCmd
}

func init() {
	benchCommandFns["multiple-setbit"] = NewMultipleSetBitsCommand
}
