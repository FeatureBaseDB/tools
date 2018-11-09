package main

import (
	"context"
	"os"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

func NewZipfCommand() *cobra.Command {
	b := bench.NewZipfBenchmark()
	cmd := &cobra.Command{
		Use:   "zipf",
		Short: "zipf sets random bits according to the Zipf distribution.",
		Long: `Sets random bits according to the Zipf distribution.

This is a power-law distribution controlled by two parameters.
Exponent, in the range (1, inf), with a default value of 1.001, controls
the "sharpness" of the distribution, with higher exponent being sharper.
Ratio, in the range (0, 1), with a default value of 0.25, controls the
maximum variation of the distribution, with higher ratio being more uniform.
`,
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
	flags.Int64Var(&b.MinRowID, "min-row-id", 0, "Rows being set will all be greater than this.")
	flags.Int64Var(&b.MaxRowID, "max-row-id", 100000, "Maximum row id for set bits.")
	flags.Int64Var(&b.MinColumnID, "min-column-id", 0, "Column id to start from.")
	flags.Int64Var(&b.MaxColumnID, "max-column-id", 100000, "Maximum column id for set bits.")
	flags.IntVar(&b.Iterations, "iterations", 100, "Number of bits to set.")
	flags.Int64Var(&b.Seed, "seed", 1, "Seed for RNG.")
	flags.StringVar(&b.Field, "field", "fbench", "Pilosa field in which to set bits.")
	flags.StringVar(&b.Index, "index", "ibench", "Pilosa index to use.")
	flags.Float64Var(&b.RowExponent, "row-exponent", 1.01, "Zipf exponent parameter for row IDs.")
	flags.Float64Var(&b.RowRatio, "row-ratio", 0.25, "Zipf probability ratio parameter for row IDs.")
	flags.Float64Var(&b.ColumnExponent, "column-exponent", 1.01, "Zipf exponent parameter for column IDs.")
	flags.Float64Var(&b.ColumnRatio, "column-ratio", 0.25, "Zipf probability ratio parameter for column IDs.")
	flags.StringVar(&b.Operation, "operation", "set", "Can be set or clear.")

	return cmd
}
