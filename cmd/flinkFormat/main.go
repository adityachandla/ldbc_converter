package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/adityachandla/ldbc_converter/adj_stage"
	"github.com/adityachandla/ldbc_converter/file_util"
)

const Parallelism = 6

var EDGE_OUT_FORMAT = "%d,%d,%d\n"

var inDir, outDir string

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Enter input and output directory.")
	}
	inDir = os.Args[1]
	if !strings.HasSuffix(inDir, "/") {
		inDir += "/"
	}
	outDir = os.Args[2]
	if !strings.HasSuffix(outDir, "/") {
		outDir += "/"
	}
	err := os.Mkdir(outDir, os.ModePerm)
	if err != nil {
		panic(err)
	}
	files, err := file_util.GetFilesInDir(inDir)
	if err != nil {
		panic(err)
	}
	fileChannel := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fileProcessor(fileChannel)
		}()
	}
	for _, file := range files {
		fileChannel <- file
	}
	close(fileChannel)
	wg.Wait()
}

func fileProcessor(fileInput <-chan string) {
	for fileName := range fileInput {
		inputPath := inDir + fileName
		file, err := os.Open(inputPath)
		if err != nil {
			panic(err)
		}

		outputPath := outDir + fileName
		outFile, err := os.Create(outputPath)

		reader := bufio.NewReader(file)
		writer := bufio.NewWriter(outFile)

		line, err := reader.ReadString('\n')
		for err == nil {
			var src, label, dest uint32
			var outgoing bool
			fmt.Sscanf(line, adj_stage.EDGE_FORMAT, &src, &label, &dest, &outgoing)
			if outgoing {
				writer.WriteString(fmt.Sprintf(EDGE_OUT_FORMAT, src, label, dest))
			}
			line, err = reader.ReadString('\n')
		}

		file.Close()
		writer.Flush()
		outFile.Close()
		fmt.Printf("Processed file %s\n", fileName)
	}
}
