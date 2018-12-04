package main

import (
	"context"
	"os"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewRandomSetCommand subcommands
func NewRandomSetCommand() *cobra.Command {
	b := bench.NewRandomSetBenchmark()
	cmd := &cobra.Command{
		Use:   "random-set",
		Short: "Executes random sets.",
		Long: `Sets random values according to the parameters using PQL through the /query endpoint.
If NumAttrs and NumAttrValues are greater than 0, then each SetBit query is
followed by a SetRowAttrs query on the same row id. Each SetRowAttrs query sets
a single attribute to an integer value chosen randomly. There will be num-attrs
total possible attributes and num-attr-values total possible values. Agent num
modifies random seed.`,

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
	flags.Int64Var(&b.MinRowID, "min-row-id", 0, "Minimum row id for set.")
	flags.Int64Var(&b.MaxRowID, "max-row-id", 100000, "Maximum row id for set.")
	flags.Int64Var(&b.MinColumnID, "min-column-id", 0, "Minimum column id for set.")
	flags.Int64Var(&b.MaxColumnID, "max-column-id", 100000, "Maximum column id for set.")
	flags.Int64Var(&b.Seed, "seed", 1, "Random seed.")
	flags.IntVar(&b.Iterations, "iterations", 100, "Number of values to set.")
	flags.IntVar(&b.BatchSize, "batch-size", 1, "Number of values to set per batch.")
	flags.IntVar(&b.NumAttrs, "num-attrs", 0, "If > 0, alternate set with setrowattrs - this number of different attributes")
	flags.IntVar(&b.NumAttrValues, "num-attr-values", 0, "If > 0, alternate set with setrowattrs - this number of different attribute values")
	flags.StringVar(&b.Field, "field", defaultField, "Field to set in.")
	flags.StringVar(&b.Index, "index", defaultIndex, "Pilosa index to use.")

	return cmd
}
