package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

var DirPath string

func init() {
	flag.StringVar(&DirPath, "dir", "out-sf1", "Path with all csv files")
}

func main() {
	flag.Parse()
	if !strings.HasSuffix(DirPath, "/") {
		DirPath = DirPath + "/"
	}
	// We will have two directories temp and out
	//os.Mkdir("temp", os.ModePerm)
	//os.Mkdir("out", os.ModePerm)

	processPerson()
}

func processPerson() {
	personPath := DirPath + "Person/"
	//resultFile := "temp/person.csv"
	files, _ := getCsvFiles(personPath)
	fmt.Println(files)
}

// Get the file names of the csv files in the
// input directory.
func getCsvFiles(path string) ([]string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Unable to open directory %s", path)
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
	// We will usually have one success file and csv files
	result := make([]string, 0, len(entries)-1)
	for i := 0; i < len(entries); i++ {
		name := entries[i].Name()
		if strings.HasSuffix(name, ".csv") {
			result = append(result, name)
		}
	}
	return result, nil
}
