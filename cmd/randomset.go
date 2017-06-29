package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewRandomSetBitsCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	randomSetBits := &bench.RandomSetBits{}
	randomSetBitsCmd := &cobra.Command{
		Use:   "random-set-bits",
		Short: "Sets random bits.",
		Long: `Sets random bits according to the parameters.
Agent num modifies random seed`,
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
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, randomSetBits)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := randomSetBitsCmd.Flags()
	flags.Int64Var(&randomSetBits.BaseBitmapID, "base-bitmap-id", 0, "Minimum row id for bits to be set.")
	flags.Int64Var(&randomSetBits.BitmapIDRange, "bitmap-id-range", 100000, "Number of rows in which to set bits.")
	flags.Int64Var(&randomSetBits.BaseProfileID, "base-profile-id", 0, "Minimum column id for bits to be set.")
	flags.Int64Var(&randomSetBits.ProfileIDRange, "profile-id-range", 100000, "Number of columsn in which to set bits.")
	flags.Int64Var(&randomSetBits.Seed, "seed", 1, "Random seed.")
	flags.IntVar(&randomSetBits.Iterations, "iterations", 100, "Number of bits to set.")
	flags.StringVar(&randomSetBits.Frame, "frame", "frame", "Frame to set bits in.")
	flags.StringVar(&randomSetBits.Index, "index", "benchindex", "Pilosa index to use.")
	flags.StringVar(&randomSetBits.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'")
	flags.StringVar(&randomSetBits.ContentType, "content-type", "protobuf", "Can be protobuf or pql.")

	return randomSetBitsCmd
}

func init() {
	benchCommandFns["random-set-bits"] = NewRandomSetBitsCommand
}
