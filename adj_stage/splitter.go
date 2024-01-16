package adj_stage

import (
	"bufio"
	"fmt"
	"sync"

	"github.com/adityachandla/ldbc_converter/file_util"
)

func splitFiles(baseDir string, sizeMb int) {
	filesToSplit, err := file_util.GetFilesLargerThan(baseDir, sizeMb)
	if err != nil {
		panic(err)
	}
	for len(filesToSplit) > 0 {
		//Split the file into two
		fmt.Printf("Splitting %d files into two\n", len(filesToSplit))
		var wg sync.WaitGroup
		for _, f := range filesToSplit {
			wg.Add(1)
			fileName := f
			go func() {
				defer wg.Done()
				splitFile(baseDir, fileName)
			}()
		}
		wg.Wait()
		filesToSplit, err = file_util.GetFilesLargerThan(baseDir, sizeMb)
		if err != nil {
			panic(err)
		}
	}
}

func splitFile(dir, fileName string) {
	var start, end uint32
	fmt.Sscanf(fileName, FILE_FORMAT, &start, &end)
	mid := findMid(dir, fileName)
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
		var src, label, dest uint32
		fmt.Sscanf(line, "(%d,%d,%d)\n", &src, &label, &dest)
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
