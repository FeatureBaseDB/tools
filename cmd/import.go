package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewImportCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	importer := bench.NewImport(stdin, stdout, stderr)
	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Runs the given PQL import against pilosa and records the results along with the duration.",
		Long: `Runs the given PQL import against pilosa and records the results along with the duration.
Agent num has no effect`,
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
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, importer)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := importCmd.Flags()
	flags.Int64Var(&importer.BaseBitmapID, "base-bitmap-id", 0, "Rows being set will all be greater than this.")
	flags.Int64Var(&importer.MaxBitmapID, "max-bitmap-id", 1000, "Rows being set will all be <= this.")
	flags.Int64Var(&importer.BaseProfileID, "base-profile-id", 0, "Columns being set will all be greater than this.")
	flags.Int64Var(&importer.MaxProfileID, "max-profile-id", 1000, "Maximum column id to generate.")
	flags.BoolVar(&importer.RandomBitmapOrder, "random-bitmap-order", false, "If this option is set, the import file will not be sorted by bitmap id.")
	flags.Int64Var(&importer.MinBitsPerMap, "min-bits-per-map", 0, "Minimum number of bits set per bitmap.")
	flags.Int64Var(&importer.MaxBitsPerMap, "max-bits-per-map", 10, "Maximum number of bits set per bitmap.")
	flags.StringVar(&importer.AgentControls, "agent-controls", "", "Can be 'height', 'width', or empty - increasing number of agents modulates row range, column range, or just sets more bits in the same range. ")
	flags.Int64Var(&importer.Seed, "seed", 0, "Random seed.")
	flags.StringVar(&importer.Index, "index", "benchindex", "Pilosa index in which to set bits.")
	flags.StringVar(&importer.Frame, "frame", "import", "Pilosa frame in which to set bits.")
	flags.IntVar(&importer.BufferSize, "buffer-size", 10000000, "Number of set bits to buffer in importer before POSTing to Pilosa.")
	return importCmd
}

func init() {
	benchCommandFns["import"] = NewImportCommand
}
