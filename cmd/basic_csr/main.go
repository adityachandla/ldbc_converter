package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/adityachandla/ldbc_converter/file_util"
)

type basicCsrFormat struct {
	start, end uint32 //End is exclusive
	nodeIndex  []uint32
	edges      []edge
}

type edge struct {
	label, dest uint32
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Enter the input directory and output directory")
	}
	inputDir := os.Args[1]
	if !strings.HasSuffix(inputDir, "/") {
		inputDir += "/"
	}
	files, err := file_util.GetCsvFiles(inputDir)
	if err != nil {
		panic(err)
	}
	outputDir := os.Args[2]
	if !strings.HasSuffix(outputDir, "/") {
		outputDir += "/"
	}
	os.Mkdir(outputDir, os.ModePerm)
	//Create one file in output directory for
	//each file in input directory
	for _, f := range files {
		newName := strings.TrimSuffix(f, ".csv") + ".csr"
		oldPath := inputDir + f
		newPath := outputDir + newName
		createCsr(oldPath, newPath)
	}
}

func createCsr(oldPath, newPath string) {
	//Read all the edges and put them into an adjacency
	adjacency := createAdjacency(oldPath)
	fmt.Println(len(adjacency))
	//Sort the adjacency, first on label, then on dest
	//After this, convert the adjacency into the csr format.
}

func createAdjacency(filePath string) map[uint32][]edge {

}
