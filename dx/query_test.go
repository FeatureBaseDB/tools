package dx

import (
	"testing"
)

func TestGenerateRandomRows(t *testing.T) {
	tests := []struct{ min, max, numRows int64 }{
		{min: 4, max: 4, numRows: 3},
		{min: 3, max: 9, numRows: 2},
		{min: 5, max: 6, numRows: 4},
	}
	for _, f := range tests {
		rows, err := generateRandomRows(f.min, f.max, f.numRows)
		if err != nil {
			t.Fatalf("generating rows for min: %v, max: %v, err: %v", f.min, f.max, err)
		}
		if int64(len(rows)) != f.numRows {
			t.Fatalf("expected %v rows, got %v", f.numRows, rows)
		}
		for _, rowNum := range rows {
			if !(f.min <= rowNum && rowNum <= f.max) {
				t.Fatalf("row num %v is not in range [%v, %v]", rowNum, f.min, f.max)
			}
		}
	}
}

func TestIndexSpec_RandomIndexField(t *testing.T) {
	fs := newFieldSpec()
	fs["field0"] = pair{min: 12, max: 13}
	is := newIndexSpec()
	is["index0"] = fs

	indexName, fieldName, err := is.randomIndexField()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if indexName != "index0" {
		t.Fatalf("expected index name: %v, got %v", "index0", indexName)
	}
	if fieldName != "field0" {
		t.Fatalf("expected field name: %v, got %v", "field0", fieldName)
	}
}
