package adj_stage

import (
	"bufio"
	"fmt"
	"sync"

	"github.com/adityachandla/ldbc_converter/file_util"
)

var FileTooLarge = fmt.Errorf("File too large to split")

type splitter struct {
	wg         sync.WaitGroup
	mu         sync.Mutex
	largeFiles map[string]struct{}
	baseDir    string
}

func splitFiles(baseDir string, sizeMb int) {
	filesToSplit, err := file_util.GetFilesLargerThan(baseDir, sizeMb)
	if err != nil {
		panic(err)
	}
	s := splitter{
		wg:         sync.WaitGroup{},
		mu:         sync.Mutex{},
		largeFiles: make(map[string]struct{}), //Files that were too large to split.
		baseDir:    baseDir,
	}
	for len(filesToSplit) > 0 {
		//Split the file into two
		fmt.Printf("Splitting %d files into two\n", len(filesToSplit))
		s.splitParallel(filesToSplit)
		largeFiles, err := file_util.GetFilesLargerThan(baseDir, sizeMb)
		if err != nil {
			panic(err)
		}
		filesToSplit = make([]string, 0, len(largeFiles))
		for _, f := range largeFiles {
			if _, ok := s.largeFiles[f]; !ok {
				filesToSplit = append(filesToSplit, f)
			}
		}
	}
}

func (s *splitter) splitParallel(files []string) {
	for _, f := range files {
		s.wg.Add(1)
		go s.split(s.baseDir, f)
	}
	s.wg.Wait()
}

func (s *splitter) split(dir, fileName string) {
	defer s.wg.Done()
	err := splitFile(s.baseDir, fileName)
	if err == FileTooLarge {
		s.mu.Lock()
		s.largeFiles[fileName] = struct{}{}
		s.mu.Unlock()
	}
}

func splitFile(dir, fileName string) error {
	var start, end uint32
	fmt.Sscanf(fileName, FILE_FORMAT, &start, &end)
	mid := findMid(dir, fileName)
	if mid == end && end-start > 1 {
		mid--
	} else if mid == start && end-start > 1 {
		mid++
	} else if mid == start || mid == end {
		fmt.Println("File too large to split")
		return FileTooLarge
	}
	low := file_util.CreateFile(fmt.Sprintf(dir+FILE_FORMAT, start, mid))
	defer low.Close()
	high := file_util.CreateFile(fmt.Sprintf(dir+FILE_FORMAT, mid, end))
	defer high.Close()
	old := file_util.Open(dir + fileName)
	defer old.Close()
	oldReader := bufio.NewReader(old)
	lowWriter := bufio.NewWriter(low)
	defer lowWriter.Flush()
	highWriter := bufio.NewWriter(high)
	defer highWriter.Flush()

	line, err := oldReader.ReadString('\n')
	for err == nil {
		var src uint32
		fmt.Sscanf(line, "(%d", &src)
		if src < start || src > end {
			e := fmt.Errorf("%d edge not within %d-%d in %s\n", src, start, end, fileName)
			panic(e)
		}
		//Higher value is not inclusive.
		if src >= mid {
			highWriter.WriteString(line)
		} else {
			lowWriter.WriteString(line)
		}
		line, err = oldReader.ReadString('\n')
	}
	file_util.RemoveDir(dir + fileName)
	return nil
}

// This function ensures that we divide the file
// into two roughly equal parts
func findMid(dir, fileName string) uint32 {
	var start, end uint32
	fmt.Sscanf(fileName, FILE_FORMAT, &start, &end)
	counter := make([]uint32, end-start+1)
	var total uint32
	fileHandle := file_util.Open(dir + fileName)
	defer fileHandle.Close()
	reader := bufio.NewReader(fileHandle)

	line, err := reader.ReadString('\n')
	for err == nil {
		var src uint32
		fmt.Sscanf(line, "(%d", &src)
		counter[src-start]++
		total++
		line, err = reader.ReadString('\n')
	}

	var targetSum uint32 = total / 2
	var runningSum, idx uint32
	for runningSum < targetSum {
		runningSum += counter[idx]
		idx++
	}
	return start + idx
}
