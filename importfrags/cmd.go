package importfrags

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"

	"github.com/pilosa/go-pilosa"
	pbuf "github.com/pilosa/go-pilosa/gopilosa_pbuf"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
)

type Main struct {
	Dir      string `help:"Directory to walk looking for fragment data."`
	Index    string
	Field    string
	Workers  int `help:"Number of worker goroutines to run."`
	Pilosa   []string
	Shards   uint64        `help:"Number of shards into which to ingest"`
	Duration time.Duration `help:"How long to run the import"`

	shardNodes    map[uint64][]pilosa.URI
	bytesImported *uint64
}

func NewMain() *Main {
	m := &Main{
		Dir:     "frags",
		Index:   "fragtest",
		Field:   "field",
		Workers: 8,
		Pilosa:  []string{"localhost:10101"},
		Shards:  10,
	}
	a := uint64(0)
	m.bytesImported = &a
	return m
}

func (m *Main) Run() error {
	rand.Seed(time.Now().UnixNano())
	fragments := make([]*roaring.Bitmap, 0)

	// walk all files in directory structure and load the ones which are roaring bitmaps.
	err := filepath.Walk(m.Dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return errors.Wrap(err, "walk func")
			}
			if info.IsDir() {
				return nil
			}
			f, err := os.Open(path)
			defer f.Close()
			if err != nil {
				log.Printf("error opening '%s': %v", path, err)
				return nil
			}
			data, err := ioutil.ReadAll(f)
			if err != nil {
				return errors.Wrap(err, "reading all")
			}
			bm := roaring.NewFileBitmap()
			err = bm.UnmarshalBinary(data)
			if err != nil {
				log.Printf("%s was not a valid roaring bitmap: %v", path, err)
			}
			fragments = append(fragments, bm)
			return nil
		})
	if err != nil {
		return errors.Wrap(err, "walking file path")
	}

	if len(fragments) == 0 {
		return errors.New("no valid bitmaps found.")
	}
	fmt.Printf("found %d bitmap files\n", len(fragments))

	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		return errors.Wrapf(err, "getting client for %v", m.Pilosa)
	}
	sch, err := client.Schema()
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}
	idx := sch.Index(m.Index)
	idx.Field(m.Field)
	err = client.SyncSchema(sch)
	if err != nil {
		return errors.Wrap(err, "syncing schema")
	}

	m.shardNodes, err = client.ExperimentalShardNodes(m.Index, m.Shards)
	if err != nil {
		return errors.Wrap(err, "getting shard nodes")
	}

	go m.reportProgress()

	eg := errgroup.Group{}
	done := make(chan struct{})
	for i := 0; i < m.Workers; i++ {
		i := i
		eg.Go(func() error {
			return m.importWorker(i, client, fragments, done)
		})
	}
	if m.Duration > 0 {
		fmt.Println("sleeping")
		time.Sleep(m.Duration)
		close(done)
	}

	return eg.Wait()
}

func (m *Main) reportProgress() {
	start := time.Now()
	last := time.Now()
	lastb := uint64(0)
	for tick := time.Tick(time.Second * 5); true; <-tick {
		b := atomic.LoadUint64(m.bytesImported)
		mbImported := float64(b) / 1024 / 1024
		lmb := float64(b-lastb) / 1024 / 1024
		fmt.Printf("imported %.2f MB. Overall %.2f MB/s, Last5: %.2f MB/s\n", mbImported, mbImported/time.Since(start).Seconds(), lmb/time.Since(last).Seconds())
		last = time.Now()
		lastb = b
	}

}

func (m *Main) importWorker(num int, client *pilosa.Client, fragments []*roaring.Bitmap, done chan struct{}) error {
	idx := num % len(fragments)
	path := fmt.Sprintf("/index/%s/field/%s/import-roaring/", m.Index, m.Field)
	headers := map[string]string{
		"Content-Type": "application/x-protobuf",
		"Accept":       "application/x-protobuf",
		"PQL-Version":  pilosa.PQLVersion,
	}
	for {
		shard := rand.Uint64() % m.Shards
		hosts, ok := m.shardNodes[shard]
		if !ok {
			panic("tried to get unknown shard")
		}

		bitmap := fragments[idx]
		data := &bytes.Buffer{}
		//start := time.Now()
		bitmap.WriteTo(data)
		// writeDur := time.Since(start)
		bytes := data.Bytes()
		req := &pbuf.ImportRoaringRequest{
			Views: []*pbuf.ImportRoaringRequestView{{Data: bytes}},
		}
		r, err := proto.Marshal(req)
		if err != nil {
			return errors.Wrap(err, "marshaling request to protobuf")
		}
		//start = time.Now()
		resp, err := client.ExperimentalDoRequest(&hosts[0], "POST", path+strconv.Itoa(int(shard)), headers, r)
		if err != nil {
			return errors.Wrap(err, "error doing request")
		}
		if resp.StatusCode != 200 {
			return errors.Errorf("bad resp from import: %v", resp)
		}
		// fmt.Printf("POST %d KB to %s shard: %d writeDur: %s postDur: %s\n", len(bytes)/1024, hosts[0].HostPort(), shard, writeDur, time.Since(start))
		atomic.AddUint64(m.bytesImported, uint64(len(bytes)))

		idx = (idx + 1) % len(fragments)
		select {
		case <-done:
			return nil
		default:
		}
	}
}
