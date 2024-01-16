package file_util

import (
	"fmt"
	"os"
	"strings"
)

// Get the file names of the csv files in the
// input directory.
// This method only returns filenames not the entire
// filepath
func GetCsvFiles(path string) ([]string, error) {
	// This method returns sorted list
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(entries)-1)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".csv") {
			// TODO delete trimedFileName := strings.Trim(e.Name(), " \n")
			filtered = append(filtered, e.Name())
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

// Gets files unsorted.
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

func RemoveDir(path string) {
	err := os.RemoveAll(path)
	if err != nil {
		panic(err)
	}
}

func CreateFile(path string) *os.File {
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	return file
}

func Open(filePath string) *os.File {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	return file

}
