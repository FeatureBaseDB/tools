package main

import (
	"fmt"
	"os"
	"time"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewReplayCommand() *cobra.Command {
	com, err := cobrafy.Command(newReplayCommand())
	if err != nil {
		panic(fmt.Sprintf("Couldn't create cobra command: %v", err))
	}
	com.Use = "replay"
	com.Short = "Replay recorded Pilosa imports."
	com.Long = `Replay recorded Pilosa imports.

The go-pilosa client contains an option which allows it to record all
imports it runs to a file (or other io.Writer). This tool takes a
Pilosa cluster and a filename containing such recorded data and
imports the data into that cluster. The cluster must already have the 
schema set up to support the recorded import data.
`

	return com
}

func newReplayCommand() *ReplayCommand {
	return &ReplayCommand{
		File:        "replay.gopilosa",
		Hosts:       []string{"localhost:10101"},
		Concurrency: 8,
	}
}

type ReplayCommand struct {
	File        string   `help:"File to read from."`
	Hosts       []string `help:"Pilosa hosts (comma separated)."`
	Concurrency int      `help:"Number of goroutines importing data." short:"n"`
}

func (r *ReplayCommand) Run() error {
	client, err := pilosa.NewClient(r.Hosts)
	if err != nil {
		return errors.Wrap(err, "creating Pilosa client")
	}
	f, err := os.Open(r.File)
	if err != nil {
		return errors.Wrap(err, "opening replay file")
	}

	start := time.Now()
	err = client.ExperimentalReplayImport(f, r.Concurrency)
	if err != nil {
		return errors.Wrap(err, "")
	}
	fmt.Printf("Done: %v", time.Since(start))
	return nil
}
