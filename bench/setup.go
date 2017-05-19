package bench

import (
	"fmt"

	pcli "github.com/pilosa/go-pilosa"
)

func initIndex(host string, indexName string, frameName string) error {
	pilosaURI, err := pcli.NewURIFromAddress(host)
	setupClient := pcli.NewClientWithURI(pilosaURI)
	index, err := pcli.NewIndex(indexName, &pcli.IndexOptions{})
	if err != nil {
		return fmt.Errorf("making index: %v", err)
	}
	err = setupClient.EnsureIndex(index)
	if err != nil {
		return fmt.Errorf("ensuring index existence: %v", err)
	}
	frame, err := index.Frame(frameName, &pcli.FrameOptions{})
	if err != nil {
		return fmt.Errorf("making frame: %v", err)
	}
	err = setupClient.EnsureFrame(frame)
	if err != nil {
		return fmt.Errorf("creating frame '%v': %v", frame, err)
	}

	return nil
}
