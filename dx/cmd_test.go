package dx

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

func SetupMain() (*Main, string) {
	path, err := ioutil.TempDir("", "dx-")
	if err != nil {
		panic(err)
	}
	m := NewMain()
	m.DataDir = path
	m.ThreadCount = 2
	m.NumQueries = 10
	m.SpecFiles = []string{filepath.Join("./testdata", "spec", "spec.toml")}

	return m, path
}

func SetupBits(holder *pilosa.Holder) {
	idx0, err := holder.CreateIndex("index0", pilosa.IndexOptions{})
	if err != nil {
		panic(err)
	}
	idx1, err := holder.CreateIndex("index1", pilosa.IndexOptions{})
	if err != nil {
		panic(err)
	}
	fld0, err := idx0.CreateField("field0")
	if err != nil {
		panic(err)
	}
	fld1, err := idx0.CreateField("field1")
	if err != nil {
		panic(err)
	}
	fld2, err := idx1.CreateField("field2")
	if err != nil {
		panic(err)
	}

	fld0.SetBit(0, 0, nil)
	fld0.SetBit(0, 1, nil)
	fld0.SetBit(0, 0, nil)
	fld0.SetBit(0, 2, nil)
	fld0.SetBit(1, 1, nil)
	fld0.SetBit(1, 12, nil)
	fld0.SetBit(2, 24, nil)
	fld1.SetBit(1, 2, nil)
	fld1.SetBit(1, 13, nil)
	fld1.SetBit(1, 65536, nil)
	fld1.SetBit(2, 12, nil)
	fld2.SetBit(3, 36, nil)
}

func TestIngest(t *testing.T) {
	m, path := SetupMain()
	defer os.RemoveAll(path)

	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()
	for _, cmd := range cluster {
		host := cmd.URL()
		m.Hosts = append(m.Hosts, host)
	}

	if err := ExecuteIngest(m); err != nil {
		t.Fatalf("executing ingest: %v", err)
	}

	index := "dx-index"
	q := "Row(field=%v)"
	expectedCols := []uint64{2, 5, 10}

	for i := 0; i < 5; i++ {
		query := fmt.Sprintf(q, i)
		response := cluster.Query(t, index, query)
		columns := response.Results[0].(*pilosa.Row).Columns()
		if !reflect.DeepEqual(columns, expectedCols) {
			t.Fatalf("row %v should have values %v, got %v", i, expectedCols, columns)
		}
	}

	for i := 5; i < 15; i++ {
		query := fmt.Sprintf(q, i)
		response := cluster.Query(t, index, query)
		columns := response.Results[0].(*pilosa.Row).Columns()
		if reflect.DeepEqual(columns, []uint64(nil)) {
			t.Fatalf("row %v should have no values, got %v", i, columns)
		}
	}
}

func TestQuery(t *testing.T) {
	m, path := SetupMain()
	defer os.RemoveAll(path)

	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	for _, cmd := range cluster {
		host := cmd.URL()
		m.Hosts = append(m.Hosts, host)
	}
	holder := cluster[0].Server.Holder()

	SetupBits(holder)

	if err := ExecuteQueries(m); err != nil {
		t.Fatalf("executing queries: %+v", err)
	}
}

func TestCompare(t *testing.T) {
	ingest0 := filepath.Join("./testdata", "ingest", "0")
	ingest1 := filepath.Join("./testdata", "ingest", "1")
	query0 := filepath.Join("./testdata", "query", "0")
	query1 := filepath.Join("./testdata", "query", "1")

	if err := ExecuteComparison(ingest0, ingest1); err != nil {
		t.Fatalf("comparing ingest: %v", err)
	}

	if err := ExecuteComparison(query0, query1); err != nil {
		t.Fatalf("comparing query: %v", err)
	}
}
