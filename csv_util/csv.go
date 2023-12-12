package csv_util

import (
	"bufio"
	"os"
	"strings"
)

const CSV_SEPARATOR = "|"
const TRIM_CUTSET = " \n"

type CsvFile struct {
	fileHandle *os.File
	reader     *bufio.Reader
	headers    []string
}

func CreateCsvFile(name string) *CsvFile {
	file, err := os.Open(name)
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(file)
	l, _ := r.ReadString('\n')
	l = strings.Trim(l, TRIM_CUTSET)
	headers := strings.Split(l, CSV_SEPARATOR)
	return &CsvFile{
		fileHandle: file,
		reader:     r,
		headers:    headers,
	}
}

func (csv *CsvFile) ReadRow() ([]string, error) {
	l, err := csv.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	l = strings.Trim(l, TRIM_CUTSET)
	return strings.Split(l, CSV_SEPARATOR), nil
}

func (csv *CsvFile) Reset() {
	csv.fileHandle.Seek(0, 0)
	csv.reader.Reset(csv.fileHandle)
	csv.reader.ReadString('\n')
}
