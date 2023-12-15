package csv_util

import (
	"bufio"
	"os"
	"slices"
	"strings"
)

const CSV_SEPARATOR = "|"
const TRIM_CUTSET = " \n"

type CsvFileReader struct {
	fileHandle *os.File
	reader     *bufio.Reader
	headers    []string
}

func CreateCsvFileReader(name string) *CsvFileReader {
	file, err := os.Open(name)
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(file)
	l, err := r.ReadString('\n')
	if err != nil {
		panic(err)
	}
	l = strings.Trim(l, TRIM_CUTSET)
	headers := strings.Split(l, CSV_SEPARATOR)
	return &CsvFileReader{
		fileHandle: file,
		reader:     r,
		headers:    headers,
	}
}

func (csv *CsvFileReader) GetHeaders() []string {
	return csv.headers
}

func (csv *CsvFileReader) ReadRow() ([]string, error) {
	l, err := csv.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	l = strings.Trim(l, TRIM_CUTSET)
	return strings.Split(l, CSV_SEPARATOR), nil
}

func (csv *CsvFileReader) ReadRowValues(
	selectedHeaders []string) ([]string, error) {
	row, err := csv.ReadRow()
	if err != nil {
		return nil, err
	}
	resultValues := make([]string, 0, len(selectedHeaders))
	for i := range csv.headers {
		if slices.Contains(selectedHeaders, csv.headers[i]) {
			resultValues = append(resultValues, row[i])
		}
	}
	return resultValues, nil
}

func (csv *CsvFileReader) Reset() {
	csv.fileHandle.Seek(0, 0)
	csv.reader.Reset(csv.fileHandle)
	csv.reader.ReadString('\n')
}

func (csv *CsvFileReader) Close() {
	csv.fileHandle.Close()
}

type CsvFileWriter struct {
	fileHandle *os.File
	writer     *bufio.Writer
}

func CreateCsvFileWriter(name string) *CsvFileWriter {
	fileHandle, err := os.Create(name)
	if err != nil {
		panic(err)
	}
	writer := bufio.NewWriter(fileHandle)
	return &CsvFileWriter{
		fileHandle: fileHandle,
		writer:     writer,
	}
}

func (csv *CsvFileWriter) WriteRow(row []string) {
	joinedRow := strings.Join(row, CSV_SEPARATOR)
	joinedRow += "\n"
	csv.writer.WriteString(joinedRow)
}

func (csv *CsvFileWriter) Close() {
	csv.writer.Flush()
	csv.fileHandle.Close()
}
