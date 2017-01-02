package mmcb

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const segment = 128 * 1024

//AddressableBuffer represents a buffer that uses *EntryAddress and *DaaEntry to mange it state. Each new itme increases current position by the allocated entry size.
//The buffer support basic CRUD operation
type AddressableBuffer struct {
	config               *BufferConfig
	currentPosition      uint64
	file                 *os.File
	bufferPointer        unsafe.Pointer
	bufferMutex          *sync.RWMutex // manages atomic  read, write, memory mapped remapping operation on bufferPointer
	addresses            []*EntryAddress
	addressMutex         *sync.Mutex //manages atomic addresses operation
	memoryMappingCounter int32
	currentPositionMutex *sync.Mutex //manager atomic operation on memoryMappingCounter
	length               int         //expected length of the buffer, if buffer expansion takes place this value will be grater than fileSize
	fileSize             int         //actual underlying mapped buffer size.
	dataSize             int64       //overall data size
	entrySize            int64       //overal; entry size
	removedCount         int64       //removed entry count
}

func (b *AddressableBuffer) mapMemory() error {
	newValue := atomic.AddInt32(&b.memoryMappingCounter, 1)
	if newValue > 1 {
		return fmt.Errorf("Can not map memory, it has been already mapped, unmapped it first ! %v\n", b.memoryMappingCounter)
	}

	err := b.updateFileSize()
	if err != nil {
		return fmt.Errorf("Failed to map memory - unable to read file size to %v", err)
	}
	buffer, err := syscall.Mmap(int(b.file.Fd()), 0, b.Len(), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("Failed to map memory due to %v, fd: %v %v", err, b.file.Fd(), b.Len())
	}
	b.setBuffer(&buffer)
	return nil
}

//FileSize returns file size.
func (b *AddressableBuffer) FileSize() int {
	return b.fileSize
}

func (b *AddressableBuffer) unmapMemory() error {
	newValue := atomic.AddInt32(&b.memoryMappingCounter, -1)
	if newValue != 0 {
		return fmt.Errorf("Can not unmap memory, map it first ! %v\n", b.memoryMappingCounter)
	}
	buffer := b.getBuffer()
	return syscall.Munmap(*buffer)
	return nil
}

//RemainingSpacePct returns remaining space pct. It uses current position of file size.
func (b *AddressableBuffer) RemainingSpacePct() float32 {
	currentPointer := atomic.LoadUint64(&b.currentPosition)
	return 100.0 * float32(currentPointer) / float32(b.FileSize())
}

//Close unmap the underlying buffer and closes associated file descriptor.
func (b *AddressableBuffer) Close() error {
	runtime.SetFinalizer(b, nil)
	err := b.unmapMemory()
	if err != nil {
		return err
	}
	return b.file.Close()
}

//Len returns buffer length.
func (b *AddressableBuffer) Len() int {
	return b.length
}

func (b *AddressableBuffer) extendMappedMemory(minIncrement int) error {
	newAllocationSize := int(b.config.ExtensionFactor * float32(b.Len()))
	candidateIncrementedSize := b.Len() + minIncrement
	if newAllocationSize < candidateIncrementedSize {
		newAllocationSize = candidateIncrementedSize
	}
	originalSize := b.Len()
	b.length = ((candidateIncrementedSize / segment) + 1) * segment
	err := b.allocateFileSpaceIfNeeded()
	if err != nil {
		b.length = originalSize
		return err
	}
	err = b.unmapMemory()
	if err != nil {
		return err
	}
	return b.mapMemory()

}

func (b *AddressableBuffer) updateFileSize() error {
	fileInfo, err := b.file.Stat()
	if err != nil {
		return fmt.Errorf("Failed to get file info %v", err)
	}
	b.fileSize = int(fileInfo.Size())
	b.length = b.FileSize()
	return nil
}

func (b *AddressableBuffer) allocateFileSpaceIfNeeded() error {

	if b.FileSize() < b.Len() {
		_, err := b.file.Seek(int64(b.Len()-1), 0)
		if err != nil {
			return fmt.Errorf("Failed to seek file %v", err)
		}
		_, err = b.file.Write([]byte{0})
		if err != nil {
			return fmt.Errorf("Failed to resize file %v", err)
		}
	}
	return nil
}

func (b *AddressableBuffer) getBuffer() *[]byte {
	pointer := atomic.LoadPointer(&b.bufferPointer)
	return (*[]byte)(pointer)
}

func (b *AddressableBuffer) setBuffer(buffer *[]byte) {
	atomic.StorePointer(&b.bufferPointer, unsafe.Pointer(buffer))
}

//ReadAt reads bytes at position specified by offset, with provided limit. It returns number of bytes read, or error.
//This operation uses bufferMutex read lock.
func (b *AddressableBuffer) ReadAt(out []byte, offset, limit uint64) (int, error) {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()
	buffer := b.getBuffer()
	if int(offset+limit) >= b.FileSize() {
		return 0, fmt.Errorf("Invalid data access: offset +  limit: %v, fileSize: %v - %v", (offset + limit), b.fileSize, io.EOF)
	}
	bytesCopied := copy(out, (*buffer)[offset:offset+limit])
	if bytesCopied < len(out) {
		return bytesCopied, io.EOF
	}
	return bytesCopied, nil
}

//Write writes passed in bytes at provided position. It returns number of bytes written or error.
//This operation is synchronized on bufferMutex write lock.
func (b *AddressableBuffer) Write(position uint64, bytes ...byte) (int, error) {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()
	maxPosition := int(position) + len(bytes)
	missingSize := maxPosition - b.FileSize()
	if missingSize > 0 {
		err := b.extendMappedMemory(missingSize)
		if err != nil {
			return 0, err
		}
	}

	buffer := b.getBuffer()
	for j, i := 0, position; j < len(bytes); i++ {
		(*buffer)[i] = bytes[j]
		j++
	}
	return maxPosition, nil
}

func validateControlByte(bytes []byte) error {
	if bytes[0] == controlVersionByte {
		return nil
	}
	return ErrInvalidControlByte
}

func (b *AddressableBuffer) acquireAddress(entry *Entry) (uint64, error) {
	b.currentPositionMutex.Lock()
	defer b.currentPositionMutex.Unlock()

	previousPosition := atomic.LoadUint64(&b.currentPosition)
	atomic.StoreUint64(&b.currentPosition, previousPosition+uint64(entry.EntrySize()))
	entry.dataAddress = previousPosition + reservedSize
	_, err := b.Write(b.currentPosition, 0x0)
	//	fmt.Printf("acquireAddress: %v %v\n",previousPosition,  b.currentPosition)
	return previousPosition, err
}

//ReadHeader reads header for passed in address.
func (b *AddressableBuffer) ReadHeader(address *EntryAddress) (*EntryHeader, error) {
	headerBytes := make([]byte, reservedSize)

	_, err := b.ReadAt(headerBytes, address.Position(), reservedSize)
	if err != nil {
		return nil, fmt.Errorf("Failed to read header data %v", err)
	}
	err = validateControlByte(headerBytes)
	if err != nil {
		return nil, err
	}
	header := &EntryHeader{}
	header.status = headerBytes[statusPosition]
	entrySize, _, err := ReadIntFromBytes(headerBytes, reservePosition)
	if err != nil {
		return nil, err
	}
	header.dataAddress = address.Position() + reservedSize
	header.entrySize = int64(entrySize)
	varIntEntrySize := VarIntSize(header.EntrySize())
	dataSizeBytes := make([]byte, varIntEntrySize)

	_, err = b.ReadAt(dataSizeBytes, header.dataAddress, uint64(varIntEntrySize))
	if err != nil {
		return nil, fmt.Errorf("Failed to read header data size %v", err)
	}

	dataSize, _, err := ReadIntFromBytes(dataSizeBytes, 0)
	if err != nil {
		return nil, err
	}
	header.dataSize = int64(dataSize)
	return header, nil
}

//ReadEntry reads entry for passed in address.
func (b *AddressableBuffer) ReadEntry(address *EntryAddress) (*Entry, error) {
	header, err := b.ReadHeader(address)
	if err != nil {
		return nil, err
	}
	varIntDataSize := VarIntSize(int(header.dataSize))
	var bytes = make([]byte, int(header.dataSize)+varIntDataSize)
	_, err = b.ReadAt(bytes, header.dataAddress, uint64(len(bytes)))
	if err != nil {
		return nil, fmt.Errorf("Failed to read entry: %v", err)
	}
	data := bytes[varIntDataSize:]
	return &Entry{EntryHeader: header, data: &data}, nil
}

func (b *AddressableBuffer) load() error {
	if b.currentPosition != datafileHeaderSize {
		return fmt.Errorf("Invalid buffer position expected %v but had %v", datafileHeaderSize, b.currentPosition)
	}
	var position = uint64(datafileHeaderSize)
	address := NewEntryAddress(b.config.BufferId, position)
	for {
		header, err := b.ReadHeader(address)
		if err != nil {
			switch err {
			case ErrInvalidControlByte:
				atomic.StoreUint64(&b.currentPosition, position)
				return nil
			default:
				return fmt.Errorf("Failed to read header due to %v", err)
			}
		}
		if header.EntrySize() == 0 {
			break
		}

		if header.IsValid() {
			b.addAddress(address)
			b.entrySize = b.entrySize + int64(header.EntrySize())
			b.dataSize = b.dataSize + int64(header.DataSize())

		} else if header.IsRemoved() {
			atomic.AddInt64(&b.removedCount, 1)
		} else {
			return fmt.Errorf("Invalid header status at address %v", address.Position())
		}

		position = position + uint64(header.EntrySize())

		address = NewEntryAddress(b.config.BufferId, position)

	}

	return nil
}

func (b *AddressableBuffer) remove(address *EntryAddress) error {
	_, err := b.Write(address.Position()+statusPosition, statusRemoved)
	if err != nil {
		return fmt.Errorf("Failed to remove entry %v", err)
	}
	atomic.AddInt64(&b.removedCount, 1)
	return nil
}

func (b *AddressableBuffer) move(address *EntryAddress, entry *Entry) error {
	bytes, err := entry.ToBytes()
	if err != nil {
		return fmt.Errorf("Invalid entry size %v", err)
	}
	bytes = append(bytes, 0x0) //writes extra 0 bytes to help scanning entries
	position, err := b.acquireAddress(entry)
	if err != nil {
		return err
	}
	_, err = b.Write(position, bytes...)
	if err != nil {
		return fmt.Errorf("Failed to write data at %v due %v", position, err)
	}
	atomic.AddInt64(&b.entrySize, int64(entry.EntrySize()))
	atomic.AddInt64(&b.dataSize, int64(entry.DataSize()))

	addressLockBit := writeUnLocked
	if address.IsWriteLocked() {
		addressLockBit = writeLocked
	}
	encodedAddress := encodeLockAddress(b.config.BufferId, address.ReadCounter(), addressLockBit, position)
	b.addAddress(address)
	atomic.StoreUint64(&address.encodedAddress, encodedAddress)

	encoded := address.readSafely()
	if !atomic.CompareAndSwapUint64(&address.encodedAddress, encoded, encodedAddress) {
		return fmt.Errorf("Failed to swap address after expanding %v \n", address)
	}
	return nil
}

func (b *AddressableBuffer) open() error {
	var openingFlag = os.O_RDWR
	var newFileFlag = false
	if _, err := os.Stat(b.config.Filename); os.IsNotExist(err) {
		openingFlag = openingFlag | os.O_CREATE
		newFileFlag = true
	}
	file, err := os.OpenFile(b.config.Filename, openingFlag, 0644)
	if err != nil {
		return fmt.Errorf("Failed to create a new file: %v %v", b.config.Filename, err)
	}
	b.file = file

	err = b.updateFileSize()
	if err != nil {
		return err
	}

	if b.fileSize < b.config.InitialSize {
		b.length = b.config.InitialSize
	}
	err = b.allocateFileSpaceIfNeeded()
	if err != nil {
		return fmt.Errorf("Failed to allocate file space %v: %v", b.config.Filename, err)
	}
	err = b.mapMemory()
	if err != nil {
		return fmt.Errorf("Failed to map file %v: %v", b.config.Filename, err)
	}

	buffer := b.getBuffer()
	if !newFileFlag {
		bufferId, _, err := ReadUIntFromBytes(*buffer, uint64(1))
		if err != nil {
			return fmt.Errorf("Failed to read id due to %v", err)
		}
		b.config.BufferId = uint8(bufferId)
	} else {
		WriteUIntToBytes(uint(b.config.BufferId), *buffer, 1)
		(*buffer)[datafileHeaderSize] = 0x0
	}
	return nil
}

//reset resets current mapped buffer
func (b *AddressableBuffer) reset() error {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()
	err := b.unmapMemory()
	if err != nil {
		return fmt.Errorf("Failed to reset buffer - unable to unmap file %v", err)
	}
	err = b.file.Close()
	if err != nil {
		return fmt.Errorf("Failed to reset buffer - unable to close file %v", err)
	}
	err = os.Remove(b.config.Filename)
	if err != nil {
		return fmt.Errorf("Failed to reset buffer - unable to remove file %v", err)
	}
	return b.open()
}

func (b *AddressableBuffer) resetAddresses(matchingCount int) bool {
	b.addressMutex.Lock()
	defer b.addressMutex.Unlock()
	if matchingCount == len(b.addresses) {
		b.addresses = make([]*EntryAddress, 0)
		return true
	}
	return false
}

func (b *AddressableBuffer) addAddress(address *EntryAddress) {
	b.addressMutex.Lock()
	defer b.addressMutex.Unlock()
	b.addresses = append(b.addresses, address)

}

//Addresses returns this buffer entry's addresses.
func (b *AddressableBuffer) Addresses() []*EntryAddress {
	b.addressMutex.Lock()
	result := b.addresses
	b.addressMutex.Unlock()
	return result
}

//Count returns addresses count.
func (b *AddressableBuffer) Count() int {
	b.addressMutex.Lock()
	result := len(b.addresses)
	b.addressMutex.Unlock()
	return result
}

//NewAddressableBuffer create a new addressable buffer or error. It takes buffer config.
func NewAddressableBuffer(config *BufferConfig) (*AddressableBuffer, error) {
	result := &AddressableBuffer{
		config:               config,
		currentPosition:      datafileHeaderSize,
		addressMutex:         &sync.Mutex{},
		currentPositionMutex: &sync.Mutex{},
		addresses:            make([]*EntryAddress, 0),
		bufferMutex:          &sync.RWMutex{}}
	err := result.open()
	if err != nil {
		return nil, fmt.Errorf("Failed to create new addressable buffer due to %v", err)
	}
	runtime.SetFinalizer(result, (*AddressableBuffer).Close)
	return result, nil
}
