package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewZipfCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	zipf := &bench.Zipf{}
	zipfCmd := &cobra.Command{
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
			hostSetup, err := bench.HostSetupFromFlags(flags)
			if err != nil {
				return err
			}
			agentNum, err := flags.GetInt("agent-num")
			if err != nil {
				return err
			}
			result := bench.RunBenchmark(context.Background(), hostSetup, agentNum, zipf)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := zipfCmd.Flags()
	flags.Int64Var(&zipf.MinRowID, "min-row-id", 0, "Rows being set will all be greater than this.")
	flags.Int64Var(&zipf.MaxRowID, "max-row-id", 100000, "Maximum row id for set bits.")
	flags.Int64Var(&zipf.MinColumnID, "min-column-id", 0, "Column id to start from.")
	flags.Int64Var(&zipf.MaxColumnID, "max-column-id", 100000, "Maximum column id for set bits.")
	flags.IntVar(&zipf.Iterations, "iterations", 100, "Number of bits to set.")
	flags.Int64Var(&zipf.Seed, "seed", 1, "Seed for RNG.")
	flags.StringVar(&zipf.Frame, "frame", "fbench", "Pilosa frame in which to set bits.")
	flags.StringVar(&zipf.Index, "index", "ibench", "Pilosa index to use.")
	flags.Float64Var(&zipf.RowExponent, "row-exponent", 1.01, "Zipf exponent parameter for row IDs.")
	flags.Float64Var(&zipf.RowRatio, "row-ratio", 0.25, "Zipf probability ratio parameter for row IDs.")
	flags.Float64Var(&zipf.ColumnExponent, "column-exponent", 1.01, "Zipf exponent parameter for column IDs.")
	flags.Float64Var(&zipf.ColumnRatio, "column-ratio", 0.25, "Zipf probability ratio parameter for column IDs.")
	flags.StringVar(&zipf.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'.")
	flags.StringVar(&zipf.Operation, "operation", "set", "Can be set or clear.")
	flags.StringVar(&zipf.ContentType, "content-type", "protobuf", "Can be protobuf or pql.")

	return zipfCmd
}

func init() {
	benchCommandFns["zipf"] = NewZipfCommand
}
