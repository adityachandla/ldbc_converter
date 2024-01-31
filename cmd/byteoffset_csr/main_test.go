package main

import (
	"github.com/adityachandla/ldbc_converter/bin_util"
	"github.com/adityachandla/ldbc_converter/file_util"
	"github.com/stretchr/testify/assert"
	"os"
	"slices"
	"testing"
)

func TestOffsets(t *testing.T) {
	edges := readEdges("test1.csv")
	slices.SortFunc(edges, adjacencyCmp)
	csr := convertToCsr(edges)
	assert.Equal(t, uint32(1), csr.start)
	assert.Equal(t, uint32(5), csr.end)

	assert.Equal(t, uint32(48), csr.nodeOffsets[0].outgoingOffset)
	assert.Equal(t, len(edges), len(csr.edges))
	assert.Equal(t, uint32(96), csr.nodeOffsets[len(csr.nodeOffsets)-1].outgoingOffset)
}

func TestNoIncomingLast(t *testing.T) {
	edges := []adjacencyEdge{{1, 1, 2, true}, {2, 2, 3, true}}
	csr := convertToCsr(edges)
	assert.Equal(t, uint32(40), csr.nodeOffsets[1].incomingOffset)
	assert.Equal(t, uint32(32), csr.nodeOffsets[1].outgoingOffset)
}

// This test performs the byte offset operations that we
// would expect the reader to perform.
func TestReadOutputFile(t *testing.T) {
	edges := readEdges("test1.csv")
	slices.SortFunc(edges, adjacencyCmp)
	csr := convertToCsr(edges)
	csr.writeToFile("testout.ocsr")
	fileHandle, _ := os.Open("testout.ocsr")
	uintBytes := make([]byte, 4)

	n, _ := fileHandle.Read(uintBytes)
	assert.Equal(t, 4, n)
	start := bin_util.ParseUint32(uintBytes)
	assert.Equal(t, uint32(1), start)

	n, _ = fileHandle.Read(uintBytes)
	assert.Equal(t, 4, n)
	end := bin_util.ParseUint32(uintBytes)
	assert.Equal(t, uint32(5), end)

	size := (end - start + 1) * SizeEdge
	nodeOffsetBytes := make([]byte, size)
	n, _ = fileHandle.Read(nodeOffsetBytes)
	assert.Equal(t, int(size), n)

	offsets := bin_util.ParseUint32Arr(nodeOffsetBytes)
	nodeOffsets := make([]nodeOffset, len(offsets)/2)
	for i := 0; i < len(nodeOffsets); i++ {
		nodeOffsets[i].outgoingOffset = offsets[2*i]
		nodeOffsets[i].incomingOffset = offsets[(2*i)+1]
	}

	nodeOneOutBytes := make([]byte, nodeOffsets[0].incomingOffset-nodeOffsets[0].outgoingOffset)
	n, _ = fileHandle.ReadAt(nodeOneOutBytes, int64(nodeOffsets[0].outgoingOffset))
	assert.Equal(t, len(nodeOneOutBytes), n)
	nodeOneOutArr := bin_util.ParseUint32Arr(nodeOneOutBytes)
	assert.Equal(t, []uint32{2, 3, 3, 2}, nodeOneOutArr)

	file_util.RemoveDir("testout.ocsr")
}
