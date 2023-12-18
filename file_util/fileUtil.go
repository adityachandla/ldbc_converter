package file_util

import (
	"fmt"
	"os"
	"strings"
)

func GetFilesInDir(path string) ([]string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Unable to open directory %s", path)
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
	fileNames := make([]string, len(entries))
	for i, e := range entries {
		fileNames[i] = e.Name()
	}
	return fileNames, nil
}

// Get the file names of the csv files in the
// input directory.
// This method only returns filenames not the entire
// filepath
func GetCsvFiles(path string) ([]string, error) {
	fileNames, err := GetFilesInDir(path)
	if err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(fileNames)-1)
	for _, fileName := range fileNames {
		if strings.HasSuffix(fileName, ".csv") {
			trimedFileName := strings.Trim(fileName, " \n")
			filtered = append(filtered, trimedFileName)
		}
	}
	return filtered, nil
}
