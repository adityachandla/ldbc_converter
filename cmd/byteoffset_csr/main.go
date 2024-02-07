package main

import (
	"bufio"
	"fmt"
	"github.com/adityachandla/ldbc_converter/adj_stage"
	"github.com/adityachandla/ldbc_converter/bin_util"
	"github.com/adityachandla/ldbc_converter/file_util"
	"os"
	"slices"
	"strings"
	"sync"
)

const SizeUintBytes = 4
const SizeNodeOffset = 2 * SizeUintBytes
const SizeEdge = 2 * SizeUintBytes
const Parallelism = 8

var inDir, outDir string

type offsetCsr struct {
	start, end  uint32
	nodeOffsets []nodeOffset
	edges       []edge
}

func (csr *offsetCsr) writeToFile(filename string) {
	writer, err := bin_util.CreateWriter(filename)
	if err != nil {
		panic(err)
	}
	defer writer.Close()
	writer.WriteUint32(csr.start)
	writer.WriteUint32(csr.end)
	for _, nodeOffset := range csr.nodeOffsets {
		writer.WriteUint32(nodeOffset.outgoingOffset)
		writer.WriteUint32(nodeOffset.incomingOffset)
	}
	for _, edge := range csr.edges {
		writer.WriteUint32(edge.label)
		writer.WriteUint32(edge.dest)
	}
}

type edge struct {
	label, dest uint32
}

type nodeOffset struct {
	outgoingOffset uint32
	incomingOffset uint32
}

func (no *nodeOffset) set(offset uint32, outgoing bool) {
	if outgoing {
		no.outgoingOffset = offset
	} else {
		no.incomingOffset = offset
	}
}

type adjacencyEdge struct {
	src, label, dest uint32
	outgoing         bool
}

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

func fileProcessor(fileChannel <-chan string) {
	for file := range fileChannel {
		edges := readEdges(inDir + file)
		slices.SortFunc(edges, adjacencyCmp)
		csr := convertToCsr(edges)
		newName := strings.TrimSuffix(file, ".csv") + ".ocsr"
		csr.writeToFile(outDir + newName)
		fmt.Printf("Processed file %s\n", outDir+newName)
	}
}

type iterationPtr struct {
	currNode uint32
	outgoing bool
}

func (ptr *iterationPtr) isSame(e adjacencyEdge) bool {
	return e.src == ptr.currNode && e.outgoing == ptr.outgoing
}

func (ptr *iterationPtr) increment() {
	if ptr.outgoing {
		ptr.outgoing = false
	} else {
		ptr.outgoing = true
		ptr.currNode++
	}
}

func convertToCsr(edges []adjacencyEdge) *offsetCsr {
	csr := &offsetCsr{}
	csr.start = edges[0].src
	csr.end = edges[len(edges)-1].src
	totalNodes := csr.end - csr.start + 1
	csr.nodeOffsets = make([]nodeOffset, totalNodes)
	csr.edges = make([]edge, 0, len(edges))

	currOffset := (2 * SizeUintBytes) + (totalNodes * SizeNodeOffset)
	csr.nodeOffsets[0].outgoingOffset = currOffset
	ptr := iterationPtr{csr.start, true}
	for _, e := range edges {
		for !ptr.isSame(e) {
			ptr.increment()
			csr.nodeOffsets[ptr.currNode-csr.start].set(currOffset, ptr.outgoing)
		}
		currOffset += SizeEdge
		csr.edges = append(csr.edges, edge{e.label, e.dest})
	}
	if ptr.outgoing {
		//The last node did not have incoming edges
		csr.nodeOffsets[len(csr.nodeOffsets)-1].set(currOffset, false)
	}
	return csr
}

func readEdges(filename string) []adjacencyEdge {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	triples := make([]adjacencyEdge, 0, 1_000)
	reader := bufio.NewReader(f)
	line, err := reader.ReadString('\n')
	for err == nil {
		var edge adjacencyEdge
		fmt.Sscanf(line, adj_stage.EDGE_FORMAT, &edge.src, &edge.label, &edge.dest, &edge.outgoing)
		triples = append(triples, edge)
		line, err = reader.ReadString('\n')
	}
	return triples
}

// Negative when t1 < t2
// Positive when t1 > t2
// 0 when equal
func adjacencyCmp(t1, t2 adjacencyEdge) int {
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
