package mmcb

import (
	"fmt"
	"io"
)

type compatableBufferReader struct {
	compactableBuffer *CompactableBuffer
	address           *EntryAddress
	position          int
}

func (r *compatableBufferReader) Read(bytes []byte) (int, error) {
	bytesRead, err := r.ReadAt(bytes, int64(r.position))
	if err == nil {
		r.position = r.position + int(bytesRead)
	}
	return bytesRead, err
}

func (r *compatableBufferReader) ReadAt(bytes []byte, offset int64) (n int, err error) {
	r.address.LockForRead()
	defer r.address.UnlockRead()
	readableBuffer, err := r.compactableBuffer.bufferForAddress(r.address)
	if err != nil {
		return 0, fmt.Errorf("Failed to read bytes %v", err)
	}
	header, err := readableBuffer.ReadHeader(r.address)
	if err != nil {
		return 0, fmt.Errorf("Failed to read bytes %v", err)
	}
	dataSizeVarInt := VarIntSize(int(header.dataSize))
	bytesToRead := header.dataSize - offset
	if header.IsRemoved() || bytesToRead <= 0 {
		return 0, io.EOF
	}

	if len(bytes) < int(bytesToRead) {
		bytesToRead = int64(len(bytes))
	}
	offset = offset + int64(header.dataAddress) + int64(dataSizeVarInt)
	return readableBuffer.ReadAt(bytes, uint64(offset), uint64(bytesToRead))
}

func newCompatableBufferReader(compactableBuffer *CompactableBuffer, address *EntryAddress) *compatableBufferReader {
	return &compatableBufferReader{
		compactableBuffer: compactableBuffer,
		address:           address,
	}
}
