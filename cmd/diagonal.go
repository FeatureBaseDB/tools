package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewDiagonalSetBitsCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	diagonalSetBits := &bench.DiagonalSetBits{}
	diagonalSetBitsCmd := &cobra.Command{
		Use:   "diagonal-set-bits",
		Short: "Sets bits with increasing column id and row id.",
		Long: `Sets bits with increasing column id and row id.

Agent num offsets both the base column id and base row id by the number of
iterations, so that only bits on the main diagonal are set, and agents don't
overlap at all.`,
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
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, diagonalSetBits)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := diagonalSetBitsCmd.Flags()
	flags.IntVar(&diagonalSetBits.BaseRowID, "base-row-id", 0, "Rows being set will all be greater than this.")
	flags.IntVar(&diagonalSetBits.BaseColumnID, "base-column-id", 0, "Columns being set will all be greater than this.")
	flags.IntVar(&diagonalSetBits.Iterations, "iterations", 100, "Number of bits to set.")
	flags.StringVar(&diagonalSetBits.Index, "index", "benchindex", "Pilosa index in which to set bits.")
	flags.StringVar(&diagonalSetBits.Frame, "frame", "frame", "Pilosa frame in which to set bits.")
	flags.StringVar(&diagonalSetBits.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'.")
	flags.StringVar(&diagonalSetBits.ContentType, "content-type", "protobuf", "Protobuf or pql.")

	return diagonalSetBitsCmd
}

func init() {
	benchCommandFns["diagonal-set-bits"] = NewDiagonalSetBitsCommand
}
