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

func GetFilesLargerThan(path string, sizeMb int) ([]string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0)
	for _, e := range entries {
		fileInfo, err := e.Info()
		if err != nil {
			return nil, err
		}
		if fileInfo.Size() > (int64(sizeMb) * 1024 * 1024) {
			files = append(files, e.Name())
		}
	}
	return files, nil
}
