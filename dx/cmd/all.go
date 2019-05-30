package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewAllCommand initializes a new ingest command for dx.
func NewAllCommand() *cobra.Command {
	ingestCmd := &cobra.Command{
		Use:   "all",
		Short: "perform both ingest and query",
		Long:  `Perform ingest and queries against both instances of Pilosa`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("ingest and query")
		},
	}
	return ingestCmd
}
