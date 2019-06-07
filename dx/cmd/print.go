package dx

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"
)

// printIngestResults prints the results of dx query.
func printIngestResults(size int64, cTime, pTime time.Duration, timeDelta float64) error {
	timeDelta = timeDelta * 100

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 10, 5, 5, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "ingest\t\tprimary\tcandidate\tdelta\t\n")
	fmt.Fprintf(w, "%v\t\t%v\t%v\t%.1f%%\t\n", size, pTime, cTime, timeDelta)
	fmt.Fprintln(w)
	if err := w.Flush(); err != nil {
		return fmt.Errorf("could not flush writer: %v", err)
	}
	return nil
}

// printQueryResults prints the results of dx query.
func printQueryResults(qBench ...*QueryBenchmark) error {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 10, 5, 5, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "queries\taccuracy\tprimary\tcandidate\tdelta\t\n")

	for _, q := range qBench {
		accuracy := q.Accuracy * 100
		timeDelta := q.TimeDelta * 100
		// average ms/op
		cAve := (float64(q.CTime) / float64(q.Queries)) / float64(1000000)
		pAve := (float64(q.PTime) / float64(q.Queries)) / float64(1000000)
		fmt.Fprintf(w, "%v\t%.1f%%\t%.3f ms/op\t%.3f ms/op\t%.1f%%\t", q.Queries, accuracy, pAve, cAve, timeDelta)
		fmt.Fprintln(w)
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("could not flush writer: %v", err)
	}
	return nil
}
