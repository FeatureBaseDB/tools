package dx

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"text/tabwriter"
)

// printIngestResults prints the results of dx query.
func printIngestResults(b *Benchmark) error {
	timeDelta := b.TimeDelta * 100

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 10, 5, 5, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "ingest\t\tprimary\tcandidate\tdelta\t\n")
	if b.Size != 0 {
		fmt.Fprintf(w, "%v\t\t%v\t%v\t%.1f%%\t\n", b.Size, b.PTime, b.CTime, timeDelta)
	} else {
		fmt.Fprintf(w, "\t\t%v\t%v\t%.1f%%\t\n", b.PTime, b.CTime, timeDelta)
	}
	fmt.Fprintln(w)
	if err := w.Flush(); err != nil {
		return errors.Wrap(err, "could not flush writer")
	}
	return nil
}

// printQueryResults prints the results of dx query.
func printQueryResults(qBench ...*Benchmark) error {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 10, 5, 5, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "queries\taccuracy\tprimary\tcandidate\tdelta\t\n")

	for _, b := range qBench {
		accuracy := b.Accuracy * 100
		timeDelta := b.TimeDelta * 100
		// average ms/op
		cAve := (float64(b.CTime) / float64(b.Size)) / float64(1000000)
		pAve := (float64(b.PTime) / float64(b.Size)) / float64(1000000)
		fmt.Fprintf(w, "%v\t%.1f%%\t%.3f ms/op\t%.3f ms/op\t%.1f%%\t", b.Size, accuracy, pAve, cAve, timeDelta)
		fmt.Fprintln(w)
	}
	if err := w.Flush(); err != nil {
		return errors.Wrap(err, "could not flush writer")
	}
	return nil
}
