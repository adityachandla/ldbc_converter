package bin_util

import (
	"bufio"
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
	mask := uint32(0x00_00_00_ff)
	for i := 0; i < 4; i++ {
		b.writer.WriteByte(byte(val & mask))
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
	b, err := br.reader.ReadByte()
	if err != nil {
		return 0, err
	}
	var val uint32
	for i := 0; i < 4; i++ {
		val |= uint32(b)
		b, err = br.reader.ReadByte()
		// A file should have bytes that are a multiple of 4
		// if that is not the case then we need to panic.
		if err != nil {
			panic("Unable to read byte.")
		}
	}
	return val, nil
}

func (br BinaryReader) Close() {
	br.file.Close()
}
