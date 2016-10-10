package mmcb

import (
	"encoding/binary"
)

//AppendUIntToBytes appends passed in integer as varint  to target, it returns number of bytes used to encode int to varint
func AppendUIntToBytes(i uint, target *[]byte) int {
	var intInBytes = make([]byte, 8)
	size := binary.PutUvarint(intInBytes, uint64(i))
	for i := 0; i < size; i++ {
		*target = append(*target, intInBytes[i])
	}
	return size
}

//WriteUIntToBytes writes passed in integer as varint  to target, it returns number of bytes used to encode int to varint
func WriteUIntToBytes(i uint, target []byte, offset int) int {
	var intInBytes = make([]byte, 8)
	size := binary.PutUvarint(intInBytes, uint64(i))
	for i := 0; i < size; i++ {
		target[offset+i] = intInBytes[i]
	}
	return size
}

//ReadUIntFromBytes reads uint from source starting from offset, it returns decoded varint, number of bytes used, error
func ReadUIntFromBytes(source []byte, offset uint64) (uint, int, error) {
	reader := NewByteReader(source, offset)
	result, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, 0, err
	}
	return uint(result), int(reader.currentPosition - offset), nil
}

//AppendIntToBytes appends passed in integer as varint  to target, it returns number of bytes used to encode int to varint
func AppendIntToBytes(i int, target *[]byte) int {
	var intInBytes = make([]byte, 8)
	size := binary.PutVarint(intInBytes, int64(i))
	for i := 0; i < size; i++ {
		*target = append(*target, intInBytes[i])
	}
	return size
}

//WriteIntToBytes writes passed in integer as varint  to target, it returns number of bytes used to encode int to varint
func WriteIntToBytes(i int, target []byte, offset uint64) int {
	var intInBytes = make([]byte, 8)
	size := binary.PutVarint(intInBytes, int64(i))
	for i := 0; i < size; i++ {
		target[int(offset)+i] = intInBytes[i]
	}
	return size
}

//VarIntSize returns number of bytes used by passed var int
func VarIntSize(i int) int {
	var intInBytes = make([]byte, 8)
	return binary.PutVarint(intInBytes, int64(i))
}

//VarUIntSize returns number of bytes used by passed varuint
func VarUIntSize(i uint) int {
	var intInBytes = make([]byte, 8)
	return binary.PutUvarint(intInBytes, uint64(i))
}

//ReadIntFromBytes reads int from source starting from offset, it returns decoded varint, number of bytes used, error
func ReadIntFromBytes(source []byte, offset uint64) (int, int, error) {
	reader := NewByteReader(source, offset)
	result, err := binary.ReadVarint(reader)
	if err != nil {
		return 0, 0, err
	}
	return int(result), int(reader.currentPosition - offset), nil
}

//AppendToBytes appends source bytes to target, it returns number of bytes used.
func AppendToBytes(source []byte, target *[]byte) int {
	appended := AppendIntToBytes(int(len(source)), target)
	for i := 0; i < len(source); i++ {
		*target = append(*target, source[i])
	}
	return appended + len(source)
}

//ReadBytes reader bytes from source to target, it returns number of bytes read or error
func ReadBytes(source []byte, sourceOffset uint64, target *[]byte) (int, error) {
	payloadSize, bytesRead, err := ReadIntFromBytes(source, sourceOffset)
	sourceOffset = sourceOffset + uint64(bytesRead)

	if err != nil {
		return 0, err
	}
	for i := 0; i < int(payloadSize); i++ {
		*target = append(*target, source[int(sourceOffset)+i])
	}
	return bytesRead + int(payloadSize), nil
}
