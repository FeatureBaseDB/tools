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
		Short: "sets random bits",
		Long:  `Agent num modifies random seed`,
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
	flags.Int64Var(&randomSetBits.BaseBitmapID, "base-bitmap-id", 0, "")
	flags.Int64Var(&randomSetBits.BitmapIDRange, "bitmap-id-range", 100000, "")
	flags.Int64Var(&randomSetBits.BaseProfileID, "base-profile-id", 0, "")
	flags.Int64Var(&randomSetBits.ProfileIDRange, "profile-id-range", 100000, "")
	flags.Int64Var(&randomSetBits.Seed, "seed", 1, "")
	flags.IntVar(&randomSetBits.Iterations, "iterations", 100, "number of bits to set")
	flags.StringVar(&randomSetBits.Frame, "frame", "frame", "frame to query")
	flags.StringVar(&randomSetBits.Index, "index", "benchindex", "pilosa index to use")
	flags.StringVar(&randomSetBits.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'")
	flags.StringVar(&randomSetBits.ContentType, "content-type", "protobuf", "protobuf or pql")

	return randomSetBitsCmd
}

func init() {
	benchCommandFns["random-set-bits"] = NewRandomSetBitsCommand
}
