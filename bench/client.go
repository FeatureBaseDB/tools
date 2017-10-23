package bench

import (
	"context"
	"errors"
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
func (h *HasClient) Init(hostSetup *HostSetup, agentNum int) error {
	var err error
	h.client, err = pcli.NewClientFromAddresses(hostSetup.Hosts, hostSetup.ClientOptions)
	if err != nil {
		return fmt.Errorf("getting client from addresses: %v", err)
	}
	h.schema, err = h.client.Schema()
	if err != nil {
		return fmt.Errorf("getting schema from pilosa: %v", err)
	}
	return nil
}

func (h *HasClient) ExecuteQuery(ctx context.Context, index, query string) (*pcli.QueryResponse, error) {
	idx, err := h.schema.Index(index, nil)
	if err != nil {
		return nil, err
	}
	pqlQuery := idx.RawQuery(query)
	resp, err := h.client.Query(pqlQuery, &pcli.QueryOptions{})
	return resp, err
}

func (h *HasClient) ExecuteRange(ctx context.Context, index, frame, field, query string) (*pcli.QueryResponse, error) {
	idx, err := h.schema.Index(index, nil)
	if err != nil {
		return nil, err
	}

	fr, err := idx.Frame(frame, &pcli.FrameOptions{})
	if err != nil {
		return nil, err
	}
	pqlQuery := pcli.NewPQLBitmapQuery(query, idx, err)
	sumQuery := fr.Sum(pqlQuery, field)
	resp, err := h.client.Query(sumQuery, &pcli.QueryOptions{})
	return resp, err

}

func (h *HasClient) InitIndex(index string, frame string) error {
	if h.schema == nil {
		return fmt.Errorf("You need to call HasClient.Init before InitIndex")
	}
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

func (h *HasClient) InitRange(index, frame, field string, minValue, maxValue int64) error {
	if h.schema == nil {
		return errors.New("You need to call HasClient.Init before InitRange.")
	}
	idx, err := h.schema.Index(index, &pcli.IndexOptions{})
	if err != nil {
		return err
	}

	options := &pcli.FrameOptions{}
	options.AddIntField(field, int(minValue), int(maxValue))
	_, err = idx.Frame(frame, options)
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
	if err != nil {
		return err
	}
	err = h.client.ImportFrame(pilosaFrame, iter, batchSize)
	return err
}

func (h *HasClient) ImportRange(index, frame, field string, iter pcli.ValueIterator, batchSize uint) error {
	pilosaIndex, err := h.schema.Index(index, nil)
	if err != nil {
		return err
	}

	pilosaFrame, err := pilosaIndex.Frame(frame, nil)
	if err != nil {
		return err
	}

	err = h.client.ImportValueFrame(pilosaFrame, field, iter, batchSize)
	return err
}

func initIndex(host string, clientOptions *pcli.ClientOptions, indexName string, frameName string) error {
	pilosaURI, err := pcli.NewURIFromAddress(host)
	if err != nil {
		return err
	}
	cluster := pcli.NewClusterWithHost(pilosaURI)
	setupClient := pcli.NewClientWithCluster(cluster, clientOptions)
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
