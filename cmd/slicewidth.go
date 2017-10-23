package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewSliceWidthCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	sliceWidth := bench.NewSliceWidth(stdin, stdout, stderr)
	sliceWidthCmd := &cobra.Command{
		Use:   "slice-width",
		Short: "Imports a given density of data uniformly over a configurable number of slices.",
		Long:  `Imports a given density of data uniformly over a configurable number of slices based on bit density and slice count`,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags := cmd.Flags()
			hostSetup, err := bench.HostSetupFromFlags(flags)
			if err != nil {
				return err
			}
			agentNum, err := flags.GetInt("agent-num")
			if err != nil {
				return err
			}
			if err != nil {
				return err
			}
			result := bench.RunBenchmark(context.Background(), hostSetup, agentNum, sliceWidth)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := sliceWidthCmd.Flags()
	flags.StringVar(&sliceWidth.Index, "index", defaultIndex, "Pilosa index to use.")
	flags.StringVar(&sliceWidth.Frame, "frame", defaultFrame, "Frame to import into.")
	flags.Float64Var(&sliceWidth.BitDensity, "bit-density", 0.1, "data density.")
	flags.Int64Var(&sliceWidth.SliceWidth, "slice-width", 1048576, "slice width, default to 2^20")
	flags.Int64Var(&sliceWidth.SliceCount, "slice-count", 1, "slice count")

	return sliceWidthCmd
}

func init() {
	benchCommandFns["slice-width"] = NewSliceWidthCommand
}
