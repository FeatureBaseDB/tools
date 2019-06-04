package dx

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewQueryCommand initializes a new ingest command for dx.
func NewQueryCommand() *cobra.Command {
	ingestCmd := &cobra.Command{
		Use:   "query",
		Short: "perform random queries",
		Long:  `Perform randomly generated queries against both instances of Pilosa`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("query up")
		},
	}
	return ingestCmd
}
