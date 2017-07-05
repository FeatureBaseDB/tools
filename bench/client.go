package bench

import (
	"context"
	"fmt"

	pcli "github.com/pilosa/go-pilosa"
)

// HasClient provides a reusable component for Benchmark implementations which
// provides the Init method, a ClientType argument and a cli internal variable.
type HasClient struct {
	client      *pcli.Client
	schema      *pcli.Schema
	ClientType  string `json:"client-type"`
	ContentType string `json:"content-type"`
}

// Init for HasClient looks at the ClientType field and creates a pilosa client
// either using the first host in the list of hosts or based on the agent
// number mod len(hosts)
func (h *HasClient) Init(hosts []string, agentNum int) error {
	var err error
	h.client, err = pcli.NewClientFromAddresses(hosts, &pcli.ClientOptions{})
	if err != nil {
		return fmt.Errorf("getting client from addresses: %v", err)
	}
	h.schema, err = h.client.Schema()
	if err != nil {
		return fmt.Errorf("getting schema from pilosa: %v", err)
	}
	return nil
}

func (h *HasClient) ExecuteQuery(ctx context.Context, index, query string) (interface{}, error) {
	idx, err := h.schema.Index(index, nil)
	if err != nil {
		return nil, err
	}
	pqlQuery := idx.RawQuery(query)
	resp, err := h.client.Query(pqlQuery, &pcli.QueryOptions{})
	return resp, err
}

func (h *HasClient) InitIndex(index string, frame string) error {
	idx, err := h.schema.Index(index, &pcli.IndexOptions{})
	if err != nil {
		return err
	}
	_, err = idx.Frame(frame, &pcli.FrameOptions{})
	if err != nil {
		return err
	}
	return h.client.SyncSchema(h.schema)
}

func (h *HasClient) Import(index, frame string, iter pcli.BitIterator, batchSize uint) error {
	pilosaIndex, err := h.schema.Index(index, nil)
	if err != nil {
		return err
	}
	pilosaFrame, err := pilosaIndex.Frame(frame, nil)
	err = h.client.ImportFrame(pilosaFrame, iter, batchSize)
	return err
}

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
