package cmd

import (
	"context"
	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
	"io"
)

// NewBenchCommand subcommands
func NewSliceWidthCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	sliceWidth := bench.NewSliceWidth(stdin, stdout, stderr)
	sliceWidthCmd := &cobra.Command{
		Use:   "slice-width",
		Short: "Repeatedly imports more rows into one or multiple slices.",
		Long: `Repeatedly imports more rows into a one or multiple slices based on bit density and slice count`,
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
			if err != nil {
				return err
			}
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, sliceWidth)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := sliceWidthCmd.Flags()
	flags.Int64Var(&sliceWidth.BaseIterations, "base-iterations", 0, "Number of iterations for first import")
	flags.StringVar(&sliceWidth.Index, "index", "benchindex", "Pilosa index to use.")
	flags.StringVar(&sliceWidth.Frame, "frame", "slice-width", "Frame to import into.")
	flags.Float64Var(&sliceWidth.BitDensity, "bit-density", 0, "data density.")
	flags.Int64Var(&sliceWidth.SliceWidth, "slice-width", 1048576, "slice width, default to 2^20")
	flags.Int64Var(&sliceWidth.SliceCount, "slice-count", 1, "slice count")

	return sliceWidthCmd
}

func init() {
	benchCommandFns["slice-width"] = NewSliceWidthCommand
}
