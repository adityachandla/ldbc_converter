package bin_util

import (
	"bufio"
	"fmt"
	"os"
)

type BinaryWriter struct {
	file   *os.File
	writer *bufio.Writer
}

func CreateWriter(fileName string) (BinaryWriter, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return BinaryWriter{}, err
	}
	writer := bufio.NewWriter(f)
	return BinaryWriter{f, writer}, nil
}

func (b BinaryWriter) WriteUint32(val uint32) {
	//First byte of array is the least significant byte of uint32
	mask := uint32(0x00_00_00_ff)
	for i := 0; i < 4; i++ {
		b.writer.WriteByte(byte((val & mask) >> (8 * i)))
		mask <<= 8
	}
}

func (b BinaryWriter) Close() {
	b.writer.Flush()
	b.file.Close()
}

type BinaryReader struct {
	file   *os.File
	reader *bufio.Reader
}

func CreateReader(filename string) (BinaryReader, error) {
	f, err := os.Open(filename)
	if err != nil {
		return BinaryReader{}, err
	}
	reader := bufio.NewReader(f)
	return BinaryReader{f, reader}, nil
}

func (br BinaryReader) ReadUint32() (uint32, error) {
	arr := make([]byte, 4)
	n, err := br.reader.Read(arr)
	if err != nil || n != 4 {
		return 0, fmt.Errorf("unable to read 4 bytes")
	}
	return ParseUint32(arr), nil
}

func ParseUint32(arr []byte) uint32 {
	if len(arr) != 4 {
		panic("Invalid size")
	}
	var val uint32
	for i := 0; i < 3; i++ {
		val |= uint32(arr[i]) << (8 * i)
	}
	return val
}

func ParseUint32Arr(arr []byte) []uint32 {
	if len(arr)%4 != 0 {
		panic("Invalid size")
	}
	res := make([]uint32, 0, len(arr)/4)
	for i := 0; i < len(arr); i += 4 {
		res = append(res, ParseUint32(arr[i:i+4]))
	}
	return res
}

func (br BinaryReader) Close() {
	br.file.Close()
}
