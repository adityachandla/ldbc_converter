package adj_stage

import (
	"bufio"
	"fmt"
	"os"
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
	fmt.Printf("Splitting %s\n", fileName)
	var start, end uint32
	fmt.Sscanf(fileName, FILE_FORMAT, &start, &end)
	mid := (start + end) / 2
	low, err := os.Create(fmt.Sprintf(dir+FILE_FORMAT, start, mid))
	if err != nil {
		panic("Unable to create file")
	}
	defer low.Close()
	high, err := os.Create(fmt.Sprintf(dir+FILE_FORMAT, mid, end))
	if err != nil {
		panic("Unable to create file")
	}
	defer high.Close()
	old, err := os.Open(dir + fileName)
	if err != nil {
		panic("Unable to open old file")
	}
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

	err = os.Remove(dir + fileName)
	if err != nil {
		panic(fmt.Errorf("Unable to remove old file\n%s", err))
	}
}
