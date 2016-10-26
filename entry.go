package mmcb

import (
	"io"
)

const (
	controlPosition = iota
	statusPosition
	reservePosition
)

//EntryHeader represents entry header
type EntryHeader struct {
	status      byte   //statuc can be removed or valid
	entrySize   int64  //entry size including header
	dataSize    int64  //data size
	dataAddress uint64 //address for this entry
}

//IsValid checks if entry is valid.
func (h *EntryHeader) IsValid() bool {
	if h.status == statusValid {
		return true
	}
	return false
}

//IsRemoved checks if entry is removed.
func (h *EntryHeader) IsRemoved() bool {
	if h.status == statusRemoved {
		return true
	}
	return false
}

//DataSize returns entry data size.
func (h *EntryHeader) DataSize() int {
	return int(h.dataSize)
}

//EntrySize returns entry size.
func (h *EntryHeader) EntrySize() int {
	return int(h.entrySize)
}

//Entry represents a data entry with header.
type Entry struct {
	*EntryHeader
	data *[]byte
}

//ToBytes create enrry bytes representation.
func (e *Entry) ToBytes() ([]byte, error) {
	var result = make([]byte, reservedSize)
	result[controlPosition] = controlVersionByte
	result[statusPosition] = e.status
	WriteIntToBytes(int(e.entrySize), result, reservePosition)
	AppendToBytes(*e.data, &result)
	if len(result) > int(e.entrySize) {
		return nil, io.EOF
	}
	return result, nil
}

//Data returns entry's data.
func (e *Entry) Data() []byte {
	return *e.data
}
