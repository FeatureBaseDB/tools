package main

import (
	"context"
	"os"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

func NewSliceWidthCommand() *cobra.Command {
	b := bench.NewSliceWidthBenchmark()
	cmd := &cobra.Command{
		Use:   "slice-width",
		Short: "Imports a given density of data uniformly over a configurable number of slices.",
		Long:  `Imports a given density of data uniformly over a configurable number of slices based on bit density and slice count`,
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
	flags.StringVar(&b.Index, "index", defaultIndex, "Pilosa index to use.")
	flags.StringVar(&b.Field, "field", defaultField, "Field to import into.")
	flags.Float64Var(&b.BitDensity, "bit-density", 0.1, "data density.")
	flags.Int64Var(&b.SliceWidth, "slice-width", 1048576, "slice width, default to 2^20")
	flags.Int64Var(&b.SliceCount, "slice-count", 1, "slice count")

	return cmd
}
