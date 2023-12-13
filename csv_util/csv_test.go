package csv_util_test

import (
	"os"
	"testing"

	"github.com/adityachandla/ldbc_converter/csv_util"
)

func TestWriter(t *testing.T) {
	filename := "writer_test.csv"
	writer := csv_util.CreateCsvFileWriter(filename)
	writer.WriteRow([]string{"name", "age"})
	writer.WriteRow([]string{"one", "22"})
	writer.Close()

	reader := csv_util.CreateCsvFileReader(filename)
	row, err := reader.ReadRow()
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	if !sliceEqual(row, []string{"one", "22"}) {
		t.Fail()
	}
	reader.Close()
	os.Remove(filename)
}

func TestReadRow(t *testing.T) {
	csv := csv_util.CreateCsvFileReader("./testfile.csv")
	defer csv.Close()
	row, err := csv.ReadRow()
	if err != nil {
		t.Fail()
	}
	if !sliceEqual(row, []string{"one", "22", "go away"}) {
		t.Fail()
	}
}

func TestReadEmpty(t *testing.T) {
	csv := csv_util.CreateCsvFileReader("./testfile.csv")
	defer csv.Close()
	row, _ := csv.ReadRow()
	row, _ = csv.ReadRow()
	row, err := csv.ReadRow()
	if err != nil {
		t.Fail()
	}
	if !sliceEqual(row, []string{"three", "", "hi"}) {
		t.Fail()
	}
}

func TestReadEmptyLast(t *testing.T) {
	csv := csv_util.CreateCsvFileReader("./testfile.csv")
	defer csv.Close()
	row, _ := csv.ReadRow()
	row, _ = csv.ReadRow()
	row, _ = csv.ReadRow()
	row, err := csv.ReadRow()
	if err != nil {
		t.Fail()
	}
	expected := []string{"four", "22", ""}
	if !sliceEqual(row, expected) {
		t.Fatalf("Expected %v but got %v\n", expected, row)
	}
}

func TestReset(t *testing.T) {
	csv := csv_util.CreateCsvFileReader("./testfile.csv")
	defer csv.Close()
	_, err := csv.ReadRow()
	if err != nil {
		t.Fatalf("Error while reading row")
	}
	csv.Reset()
	row, err := csv.ReadRow()
	if err != nil {
		t.Fatalf("Error while reading row")
	}
	expected := []string{"one", "22", "go away"}
	if !sliceEqual(row, expected) {
		t.Fatalf("Expected %v Got %v", expected, row)
	}
}

func sliceEqual(one []string, two []string) bool {
	if len(one) != len(two) {
		return false
	}
	for i := 0; i < len(one); i++ {
		if one[i] != two[i] {
			return false
		}
	}
	return true
}
