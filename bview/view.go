package bview

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pilosa/tools/bench"
)

type Main struct {
	Bucket string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

type Spawn struct {
	Output       bench.SpawnResult
	LastModified *time.Time
}

func (m *Main) Run() error {
	S3 := s3.New(session.New())
	listObjOutput, err := S3.ListObjects(&s3.ListObjectsInput{
		Bucket: &m.Bucket,
	})
	if err != nil {
		return fmt.Errorf("listing bucket objects: %v", err)
	}

	marks := make([]*Spawn, 0, len(listObjOutput.Contents))
	for _, obj := range listObjOutput.Contents {
		objOutput, err := S3.GetObject(&s3.GetObjectInput{
			Bucket: &m.Bucket,
			Key:    obj.Key,
		})
		if err != nil {
			fmt.Fprintf(m.Stderr, "Error fetching object at key: %v, err: %v", obj.Key, err)
			continue
		}
		benchmark := &Spawn{}
		dec := json.NewDecoder(objOutput.Body)
		err = dec.Decode(&benchmark.Output)
		if err != nil {
			fmt.Fprintf(m.Stderr, "Error decoding benchmark object at key: %v, err: %v", obj.Key, err)
			continue
		}
		benchmark.LastModified = objOutput.LastModified
		marks = append(marks, benchmark)
	}

	Summarize(marks, m.Stdout)
	return nil
}

func Summarize(marks []*Spawn, out io.Writer) {
	fmt.Fprintf(out, "%6.6s | %20.20s | pilosa hosts | agent hosts |\n", "uuid", "time")
	fmt.Fprintf(out, "\t%12.12s | %18s | %16s | %16s | %16s\n", "name", "total-runtime", "min", "max", "mean")
	for _, mark := range marks {
		if len(mark.Output.BenchmarkResults) == 0 {
			continue
		}
		fmt.Fprintf(out, "%6.6s | %20.20s | %.50s | %.50s |\n", mark.Output.RunUUID, mark.LastModified.Format("Jan 2 2006 15:04:05"), mark.Output.Configuration.PilosaHosts, mark.Output.Configuration.AgentHosts)
		for idx, v := range mark.Output.BenchmarkResults {
			for agentNum, res := range v.AgentResults {
				fmt.Fprintf(out, " %v\t%38.38s | %16s | %16s | %16s | %16s \n", mark.Output.Configuration.Benchmarks[idx].Args[0], v.BenchmarkName+"-"+strconv.Itoa(agentNum), res.Duration, res.Stats.Min, res.Stats.Max, res.Stats.Mean)
			}
		}
		fmt.Fprintf(out, "\n")
	}
}
