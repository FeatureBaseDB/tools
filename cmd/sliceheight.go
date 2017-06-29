package cmd

import (
	"context"
	"io"
	"time"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewSliceHeightCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	sliceHeight := bench.NewSliceHeight(stdin, stdout, stderr)
	sliceHeightCmd := &cobra.Command{
		Use:   "slice-height",
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
			sliceHeight.MaxTime = time.Duration(maxTime) * time.Second
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, sliceHeight)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := sliceHeightCmd.Flags()
	flags.Int("max-time", 30, "Stop benchmark after this many seconds.")
	flags.Int64Var(&sliceHeight.MinBitsPerMap, "min-bits-per-map", 0, "Minimum number of bits set per row.")
	flags.Int64Var(&sliceHeight.MaxBitsPerMap, "max-bits-per-map", 10, "Maximum number of bits set per row.")
	flags.Int64Var(&sliceHeight.Seed, "seed", 0, "Seed for RNG.")
	flags.StringVar(&sliceHeight.Index, "index", "benchindex", "Pilosa index to use.")
	flags.StringVar(&sliceHeight.Frame, "frame", "slice-height", "Frame to import into.")

	return sliceHeightCmd
}

func init() {
	benchCommandFns["slice-height"] = NewSliceHeightCommand
}
