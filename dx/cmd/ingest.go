package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewIngestCommand initializes a new ingest command for dx.
func NewIngestCommand() *cobra.Command {
	ingestCmd := &cobra.Command{
		Use:   "ingest",
		Short: "ingest randomly generated data",
		Long:  `Ingest randomly generated data from imagine tool in both instances of Pilosa`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("nom nom")
		},
	}
	return ingestCmd
}
