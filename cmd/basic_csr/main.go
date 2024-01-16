package main

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/adityachandla/ldbc_converter/adj_stage"
	"github.com/adityachandla/ldbc_converter/bin_util"
	"github.com/adityachandla/ldbc_converter/file_util"
)

type basicCsrFormat struct {
	start, end  uint32 //End is exclusive
	nodeIndices []nodeIndex
	edges       []edge
}

type nodeIndex struct {
	outgoing, incoming uint32
}

func (csr *basicCsrFormat) writeToFile(filename string) {
	bw, err := bin_util.CreateWriter(filename)
	if err != nil {
		panic(err)
	}
	defer bw.Close()
	bw.WriteUint32(csr.start)
	bw.WriteUint32(csr.end)
	for i := range csr.nodeIndices {
		bw.WriteUint32(csr.nodeIndices[i].outgoing)
		bw.WriteUint32(csr.nodeIndices[i].incoming)
	}
	for i := range csr.edges {
		bw.WriteUint32(csr.edges[i].label)
		bw.WriteUint32(csr.edges[i].dest)
	}
}

type edge struct {
	label, dest uint32
	outgoing    bool
}

type triple struct {
	src, label, dest uint32
	outgoing         bool
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
		oldPath := inputDir + f
		csr := createCsr(oldPath)
		newName := strings.TrimSuffix(f, ".csv") + ".csr"
		newPath := outputDir + newName
		csr.writeToFile(newPath)
		fmt.Printf("Converted %s to csr format\n", oldPath)
	}
}

func createCsr(oldPath string) *basicCsrFormat {
	//Read all the edges.
	triples := readTriples(oldPath)
	//Sort the edges, first on src, then on label, then on dest.
	slices.SortFunc(triples, triplesCmp)
	//After this, convert to the basic Csr format
	return makeCsrFormat(triples)
}

func makeCsrFormat(triples []triple) *basicCsrFormat {
	var csr basicCsrFormat
	csr.start = triples[0].src
	csr.end = triples[len(triples)-1].src
	numVertices := csr.end - csr.start + 1
	csr.edges = make([]edge, len(triples))
	csr.nodeIndices = make([]nodeIndex, numVertices)
	currNode := triples[0].src
	csr.nodeIndices[currNode-csr.start].outgoing = 0
	outgoing := true
	for i := range triples {
		csr.edges[i] = edge{triples[i].label, triples[i].dest, triples[i].outgoing}
		//TODO fix this mess
		//We check if we have made the switch to a new node or simply
		//from outgoing to incoming
		if triples[i].src != currNode {
			//Last index did not have any incoming edges
			if outgoing {
				csr.nodeIndices[currNode-csr.start].incoming = uint32(i)
			}
			currNode = triples[i].src
			csr.nodeIndices[currNode-csr.start].outgoing = uint32(i)
			outgoing = true
		} else if outgoing && !triples[i].outgoing {
			csr.nodeIndices[currNode-csr.start].incoming = uint32(i)
			outgoing = false
		}
	}
	//It is possible that the last index did not have any incoming edges
	if outgoing {
		csr.nodeIndices[len(csr.nodeIndices)-1].incoming = uint32(len(triples) - 1)
	}
	return &csr
}

// Negative when t1 < t2
// Positive when t1 > t2
// 0 when equal
func triplesCmp(t1, t2 triple) int {
	if t1.src > t2.src {
		return 1
	} else if t1.src < t2.src {
		return -1
	}
	//Outgoing edge first and then incoming
	if t1.outgoing && !t2.outgoing {
		return -1
	} else if !t1.outgoing && t2.outgoing {
		return 1
	}
	//Src is the same, check for labels
	if t1.label > t2.label {
		return 1
	} else if t1.label < t2.label {
		return -1
	}
	//Src and label is the same, check for dest
	if t1.dest > t2.dest {
		return 1
	} else if t1.dest < t2.dest {
		return -1
	}

	//All equal
	return 0
}

func readTriples(path string) []triple {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	triples := make([]triple, 0, 1_000)
	reader := bufio.NewReader(f)
	line, err := reader.ReadString('\n')
	for err == nil {
		var t triple
		fmt.Sscanf(line, adj_stage.EDGE_FORMAT, &t.src, &t.label, &t.dest, &t.outgoing)
		triples = append(triples, t)
		line, err = reader.ReadString('\n')
	}
	return triples
}
