package main

import (
	"fmt"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/tools/importfrags"
	"github.com/spf13/cobra"
)

func NewImportFragsCommand() *cobra.Command {
	com, err := cobrafy.Command(importfrags.NewMain())
	if err != nil {
		panic(fmt.Sprintf("Couldn't create cobra command: %v", err))
	}
	com.Use = "importfrags"
	com.Short = "importfrags imports pilosa fragment files to shards."
	com.Long = `importfrags imports pilosa fragment files to shards.

TODO
`

	return com
}
