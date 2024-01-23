package main

import (
	"os"
	"testing"

	"github.com/adityachandla/ldbc_converter/bin_util"
	"github.com/stretchr/testify/assert"
)

func TestCsrCreation(t *testing.T) {
	csr := createCsr("testfile.csv")
	expectedEdges := []edge{
		{2, 3}, {1, 5}, {1, 7}, {2, 4}, //Node 10
		{2, 2}, {2, 5}, {3, 1}, //Node 11
		{2, 8}, //Node 12
		//Node 13 missing
		{3, 11}} //Node 14
	assert.Equal(t, len(csr.edges), len(expectedEdges))
	assert.Equal(t, csr.edges, expectedEdges)
	assert.Equal(t, csr.nodeIndices, []nodeIndex{{0, 1}, {4, 7}, {7, 7}, {8, 8}, {8, 9}})
}

func TestWriteToFile(t *testing.T) {
	csr := createCsr("testfile.csv")
	csr.writeToFile("test.csr")
	reader, err := bin_util.CreateReader("test.csr")
	defer os.Remove("test.csr")
	assert.NoError(t, err)
	//start and end
	assert.Equal(t, uint32(10), read(t, reader))
	assert.Equal(t, uint32(13), read(t, reader))

	//node indices
	assert.Equal(t, uint32(0), read(t, reader))
	assert.Equal(t, uint32(1), read(t, reader))

	assert.Equal(t, uint32(4), read(t, reader))
	assert.Equal(t, uint32(6), read(t, reader))

	assert.Equal(t, uint32(7), read(t, reader))
	assert.Equal(t, uint32(8), read(t, reader))

	assert.Equal(t, uint32(8), read(t, reader))
	assert.Equal(t, uint32(9), read(t, reader))

	//First edge
	assert.Equal(t, uint32(2), read(t, reader))
	assert.Equal(t, uint32(3), read(t, reader))
}

func read(t *testing.T, reader bin_util.BinaryReader) uint32 {
	v, err := reader.ReadUint32()
	assert.NoError(t, err)
	return v
}
