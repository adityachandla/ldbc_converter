package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/adityachandla/ldbc_converter/csv_util"
)

var DirPath string

const TEMP_DIR = "temp/"

func init() {
	flag.StringVar(&DirPath, "dir", "out-sf1/", "Path with all csv files")
}

func main() {
	flag.Parse()
	if !strings.HasSuffix(DirPath, "/") {
		DirPath = DirPath + "/"
	}
	// We will have two directories temp and out
	os.Mkdir(TEMP_DIR, os.ModePerm)
	//os.Mkdir("out", os.ModePerm)

	//We need input file, output file and headers
	personHeaders := []string{"id", "LocationCityId"}
	inPath := DirPath + "Person/"
	outFile := TEMP_DIR + "person.csv"
	mapCsv(personHeaders, inPath, outFile)

	knowsHeaders := []string{"Person1Id", "Person2Id"}
	inPath = DirPath + "Person_knows_Person/"
	outFile = TEMP_DIR + "personKnows.csv"
	mapCsv(knowsHeaders, inPath, outFile)
}

// Combines input files in all directories and outputs a
// single file that only contains the headers that are passed
// into the function.
func mapCsv(header []string, inDir, outFile string) {
	resultWriter := csv_util.CreateCsvFileWriter(outFile)
	defer resultWriter.Close()
	resultWriter.WriteRow(header)
	files, err := getCsvFiles(inDir)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		inputFilePath := inDir + f
		reader := csv_util.CreateCsvFileReader(inputFilePath)
		res, err := reader.ReadRowValues(header)
		for err == nil {
			resultWriter.WriteRow(res)
			res, err = reader.ReadRowValues(header)
		}
		reader.Close()
	}
}

// Get the file names of the csv files in the
// input directory.
// This method only returns filenames not the entire
// filepath
func getCsvFiles(path string) ([]string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Unable to open directory %s", path)
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
	// We will usually have one success file and remaining csv files
	result := make([]string, 0, len(entries)-1)
	for i := 0; i < len(entries); i++ {
		name := entries[i].Name()
		if strings.HasSuffix(name, ".csv") {
			result = append(result, name)
		}
	}
	return result, nil
}
