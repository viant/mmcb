package mmcb

import "io"

//Reader represents a slice reader.
type Reader struct {
	currentPosition uint64
	bytes           []byte
}

//ReadByte reads byte
func (r *Reader) ReadByte() (byte, error) {
	if r.currentPosition >= uint64(len(r.bytes)) {
		return 0, io.EOF
	}
	b := r.bytes[r.currentPosition]
	r.currentPosition++
	return b, nil
}

//NewByteReader create a new ByteReader instance.
func NewByteReader(bytes []byte, offset uint64) *Reader {
	return &Reader{currentPosition: offset, bytes: bytes}
}
