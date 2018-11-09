package main

import (
	"context"
	"os"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

func NewImportRangeCommand() *cobra.Command {
	b := bench.NewImportRangeBenchmark()
	cmd := &cobra.Command{
		Use:   "import-range",
		Short: "Import random field data into Pilosa.",
		Long:  `import-range generates random data which can be controlled by command line flags and streams it into Pilosa's /import endpoint. Agent num has no effect`,
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
	flags.Int64Var(&b.MinValue, "min-value", 0, "Minimum row id of set bits.")
	flags.Int64Var(&b.MinColumnID, "min-column-id", 0, "Minimum column id of set bits.")
	flags.Int64Var(&b.MaxValue, "max-value", 1000, "Maximum row id of set bits.")
	flags.Int64Var(&b.MaxColumnID, "max-column-id", 1000, "Maximum column id of set bits.")
	flags.Int64Var(&b.Iterations, "iterations", 1000, "Number of bits to set")
	flags.Int64Var(&b.Seed, "seed", 0, "Random seed.")
	flags.StringVar(&b.Index, "index", defaultIndex, "Pilosa index in which to set bits.")
	flags.StringVar(&b.Field, "field", defaultField, "Pilosa field in which to set bits.")
	flags.StringVar(&b.Distribution, "distribution", "uniform", "Random distribution for deltas between set bits (exponential or uniform).")
	flags.IntVar(&b.BufferSize, "buffer-size", 10000000, "Number of set bits to buffer in importer before POSTing to Pilosa.")

	return cmd
}
