package main

import (
	"context"
	"os"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

func NewDiagonalSetBitsCommand() *cobra.Command {
	b := bench.NewDiagonalSetBitsBenchmark()
	cmd := &cobra.Command{
		Use:   "diagonal-set-bits",
		Short: "Sets bits with increasing column id and row id.",
		Long: `Sets bits with increasing column id and row id.

Agent num offsets both the min column id and min row id by the number of
iterations, so that only bits on the main diagonal are set, and agents don't
overlap at all.`,
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
	flags.IntVar(&b.MinRowID, "min-row-id", 0, "Rows being set will all be greater than this.")
	flags.IntVar(&b.MinColumnID, "min-column-id", 0, "Columns being set will all be greater than this.")
	flags.IntVar(&b.Iterations, "iterations", 100, "Number of bits to set.")
	flags.StringVar(&b.Index, "index", defaultIndex, "Pilosa index in which to set bits.")
	flags.StringVar(&b.Field, "field", defaultField, "Pilosa field in which to set bits.")

	return cmd
}
