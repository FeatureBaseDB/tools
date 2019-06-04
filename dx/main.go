package main

import (
	"fmt"
	"os"

	dx "github.com/pilosa/tools/dx/cmd"
)

func main() {
	if err := dx.NewRootCmd().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}