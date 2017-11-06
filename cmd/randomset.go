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
		Long: `Sets random bits according to the parameters using PQL through the /query endpoint.
If NumAttrs and NumAttrValues are greater than 0, then each SetBit query is
followed by a SetRowAttrs query on the same row id. Each SetRowAttrs query sets
a single attribute to an integer value chosen randomly. There will be num-attrs
total possible attributes and num-attr-values total possible values. Agent num
modifies random seed.`,

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
			result := bench.RunBenchmark(context.Background(), hostSetup, agentNum, randomSetBits)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := randomSetBitsCmd.Flags()
	flags.Int64Var(&randomSetBits.MinRowID, "min-row-id", 0, "Minimum row id for set bits.")
	flags.Int64Var(&randomSetBits.MaxRowID, "max-row-id", 100000, "Maximum row id for set bits.")
	flags.Int64Var(&randomSetBits.MinColumnID, "min-column-id", 0, "Minimum column id for set bits.")
	flags.Int64Var(&randomSetBits.MaxColumnID, "max-column-id", 100000, "Maximum column id for set bits.")
	flags.Int64Var(&randomSetBits.Seed, "seed", 1, "Random seed.")
	flags.IntVar(&randomSetBits.Iterations, "iterations", 100, "Number of bits to set.")
	flags.IntVar(&randomSetBits.BatchSize, "batch-size", 1, "Number of bits to set per batch.")
	flags.IntVar(&randomSetBits.NumAttrs, "num-attrs", 0, "If > 0, alternate setbits with setrowattrs - this number of different attributes")
	flags.IntVar(&randomSetBits.NumAttrValues, "num-attr-values", 0, "If > 0, alternate setbits with setrowattrs - this number of different attribute values")
	flags.StringVar(&randomSetBits.Frame, "frame", defaultFrame, "Frame to set bits in.")
	flags.StringVar(&randomSetBits.Index, "index", defaultIndex, "Pilosa index to use.")
	flags.StringVar(&randomSetBits.ClientType, "client-type", "single", "Can be 'single' (all agents hitting one host) or 'round_robin'")
	flags.StringVar(&randomSetBits.ContentType, "content-type", "protobuf", "Can be protobuf or pql.")

	return randomSetBitsCmd
}

func init() {
	benchCommandFns["random-set-bits"] = NewRandomSetBitsCommand
}
