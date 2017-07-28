package cmd

import (
	"context"
	"io"
	"time"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewSliceWidthCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	sliceWidth := bench.NewSliceWidth(stdin, stdout, stderr)
	sliceWidthCmd := &cobra.Command{
		Use:   "slice-width",
		Short: "Repeatedly imports more rows into a single slice and tests query times in between.",
		Long: `Repeatedly imports more rows into a single slice and tests query times in between.
Agent number has no effect.`,
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
			maxTime, err := flags.GetInt("max-time")
			if err != nil {
				return err
			}
			sliceWidth.MaxTime = time.Duration(maxTime) * time.Second
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, sliceWidth)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := sliceWidthCmd.Flags()
	flags.Int("max-time", 30, "Stop benchmark after this many seconds.")
	flags.Int64Var(&sliceWidth.BaseIterations, "base-iterations", 100, "Number of iterations for first import - grows by 10x each round.")
	flags.Int64Var(&sliceWidth.Seed, "seed", 0, "Seed for RNG.")
	flags.StringVar(&sliceWidth.Index, "index", "benchindex", "Pilosa index to use.")
	flags.StringVar(&sliceWidth.Frame, "frame", "slice-width", "Frame to import into.")
	flags.Float64Var(&sliceWidth.BitDensity, "bit-density", 0, "data density.")
	flags.Int64Var(&sliceWidth.MaxColumnID, "max-column-id", 1048576, "slice width, default to 2^20")
	flags.Int64Var(&sliceWidth.SliceCount, "slice-count", 1, "slice count")

	return sliceWidthCmd
}

func init() {
	benchCommandFns["slice-width"] = NewSliceWidthCommand
}
