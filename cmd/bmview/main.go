package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pilosa/tools/bench"
)

type Spawn struct {
	Output       bench.SpawnResult
	LastModified *time.Time
}

func main() {
	S3 := s3.New(session.New())
	bucketName := "benchmarks-pilosa"
	listObjOutput, err := S3.ListObjects(&s3.ListObjectsInput{
		Bucket: &bucketName,
	})
	if err != nil {
		log.Fatal(err)
	}

	marks := make([]*Spawn, 0, len(listObjOutput.Contents))
	for _, obj := range listObjOutput.Contents {
		objOutput, err := S3.GetObject(&s3.GetObjectInput{
			Bucket: &bucketName,
			Key:    obj.Key,
		})
		if err != nil {
			log.Printf("Error fetching object at key: %v, err: %v", obj.Key, err)
			continue
		}
		benchmark := &Spawn{}
		dec := json.NewDecoder(objOutput.Body)
		err = dec.Decode(&benchmark.Output)
		if err != nil {
			log.Printf("Error decoding benchmark object at key: %v, err: %v", obj.Key, err)
			continue
		}
		benchmark.LastModified = objOutput.LastModified
		marks = append(marks, benchmark)
	}

	// OUTPUT
	fmt.Printf("%6.6s | %20.20s |\n", "uuid", "time")
	fmt.Printf("\t%12.12s | %18s |\n", "name", "total-runtime")
	for _, mark := range marks {
		if len(mark.Output.BenchmarkResults) == 0 {
			continue
		}
		fmt.Printf("%6.6s | %20.20s |\n", mark.Output.RunUUID, mark.LastModified.Format("Jan 2 2006 15:04:05"))
		for _, v := range mark.Output.BenchmarkResults {
			for agentNum, res := range v.AgentResults {
				fmt.Printf("\t%12.12s | %18s |\n", v.BenchmarkName+"-"+strconv.Itoa(agentNum), res.Duration)
			}
		}
		fmt.Printf("\n")
	}
}
