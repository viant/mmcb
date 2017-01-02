package mmcb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const controlVersionByte = 0x7F
const statusValid = 0x1
const statusRemoved = 0x2

const zeroReadCounter = int16(0)
const reservedSize = 8
const datafileHeaderSize = 16

var compactionBase uint8 = 64
var emptyAddressableBuffer *AddressableBuffer

//ErrInvalidBufferID represents data entry discrpency with owning buffer id.
var ErrInvalidBufferID = errors.New("Invalid buffer id")

//ErrInvalidControlByte represents data entry corruption.
var ErrInvalidControlByte = errors.New("Invalid control byte")

//ErrOnGoingCompaction represents ongoing compaction error (addresses can not be read at that time)
var ErrOnGoingCompaction = errors.New("On going compaction - writable and readable buffer are not the same")

//CompactableBuffer represents compactable buffer. During compaction the buffer manager separate writable and readable buffer,
// so all new writes takes place in new buffer, while data being moved from old buffer to new buffer.
type CompactableBuffer struct {
	config                *CompatbleBufferConfig
	writableBufferPointer unsafe.Pointer
	readableBufferPointer unsafe.Pointer
	tempBufferPointer     unsafe.Pointer
	entrySize             int64
	dataSize              int64
	removedCount          int64
	count                 int64
	autoCompactionEnabled int32
	notification          chan bool
	compactionWaitGroup   *sync.WaitGroup
}

//Addresses returns addresses of writable buffer or error if buffer is being compacted
func (b *CompactableBuffer) Addresses() ([]*EntryAddress, error) {
	if b.writableBufferPointer == b.readableBufferPointer {
		writableBuffer := b.writableBuffer()
		return writableBuffer.Addresses(), nil
	}
	return nil, ErrOnGoingCompaction
}

func (b *CompactableBuffer) setWritableBuffer(entryBuffer *AddressableBuffer) {
	atomic.StorePointer(&b.writableBufferPointer, unsafe.Pointer(entryBuffer))
}

func (b *CompactableBuffer) writableBuffer() *AddressableBuffer {
	pointer := atomic.LoadPointer(&b.writableBufferPointer)
	return (*AddressableBuffer)(pointer)
}

func (b *CompactableBuffer) setReadableBuffer(entryBuffer *AddressableBuffer) {
	atomic.StorePointer(&b.readableBufferPointer, unsafe.Pointer(entryBuffer))
}

func (b *CompactableBuffer) readableBuffer() *AddressableBuffer {
	pointer := atomic.LoadPointer(&b.readableBufferPointer)
	return (*AddressableBuffer)(pointer)
}

func (b *CompactableBuffer) bufferForAddress(address *EntryAddress) (*AddressableBuffer, error) {
	for i := 0; i < 3; i++ {
		pointer := atomic.LoadPointer(&b.readableBufferPointer)
		candidate := (*AddressableBuffer)(pointer)
		if candidate.config.BufferId == address.BufferId() {
			return candidate, nil
		}
		pointer = atomic.LoadPointer(&b.writableBufferPointer)
		candidate = (*AddressableBuffer)(pointer)
		if candidate.config.BufferId == address.BufferId() {
			return candidate, nil
		}
		pointer = atomic.LoadPointer(&b.tempBufferPointer)
		candidate = (*AddressableBuffer)(pointer)
		if candidate == emptyAddressableBuffer {
			return nil, ErrInvalidBufferID
		}
		if candidate.config.BufferId == address.BufferId() {
			return candidate, nil
		}
		time.Sleep(time.Microsecond)
	}
	return nil, ErrInvalidBufferID
}

//Close closes underlying buffers.
func (b *CompactableBuffer) Close() error {
	if atomic.CompareAndSwapInt32(&b.autoCompactionEnabled, 1, 0) {
		b.notification <- true
		b.compactionWaitGroup.Wait()
	}
	readable := b.readableBuffer()
	writable := b.writableBuffer()
	err := writable.Close()
	if err != nil {
		return err
	}
	if readable != writable {
		return readable.Close()
	}

	return nil
}

//DeletionPct returns DeletionPct
func (b *CompactableBuffer) DeletionPct() float32 {
	return 100.0 * float32(b.RemovedCount()) / float32(b.Count())
}

//DataSize returns data size.
func (b *CompactableBuffer) DataSize() int {
	return int(b.dataSize)
}

//EntrySize returns entry size.
func (b *CompactableBuffer) EntrySize() int {
	return int(b.entrySize)
}

//Count returns data count.
func (b *CompactableBuffer) Count() int {
	return int(atomic.LoadInt64(&b.count))
}

//RemovedCount returns removed count.
func (b *CompactableBuffer) RemovedCount() int {
	return int(atomic.LoadInt64(&b.removedCount))
}

func (b *CompactableBuffer) expand(address *EntryAddress, data []byte) error {
	previousAddress := address.Clone()
	mewAddress, err := b.Add(data)
	if err != nil {
		return fmt.Errorf("Failed to update - unable to reallocate due to %v", err)
	}

	encodedAddress := encodeLockAddress(mewAddress.BufferId(), previousAddress.ReadCounter(), writeLocked, mewAddress.Position())

	if !atomic.CompareAndSwapUint64(&address.encodedAddress, previousAddress.encodedAddress, encodedAddress) {
		return fmt.Errorf("Failed to swap address after expand %v, %v\n", previousAddress, address)
	}
	return b.removeWithoutLock(previousAddress)
}

//Add adds data to the writable buffer
func (b *CompactableBuffer) Add(data []byte) (*EntryAddress, error) {
	entry := b.allocateEntry(data)
	bytes, err := entry.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("Invalid entry size %v", err)
	}
	if len(bytes) >= int(entry.entrySize) {
		return nil, fmt.Errorf("Invalid entry size e:%v,h:%v", len(bytes), int(entry.entrySize))
	}

	writableBuffer := b.writableBuffer()
	position, err := writableBuffer.acquireAddress(entry)
	if err != nil {
		return nil, err
	}
	_, err = writableBuffer.Write(position, bytes...)
	if err != nil {
		return nil, fmt.Errorf("Failed to write data at %v due %v", position, err)
	}
	atomic.AddInt64(&b.count, 1)
	atomic.AddInt64(&b.entrySize, entry.entrySize)
	atomic.AddInt64(&b.dataSize, entry.dataSize)

	result := NewEntryAddress(writableBuffer.config.BufferId, position)

	writableBuffer.addAddress(result)
	return result, nil
}

//Update buffer for passed in address with provided data, or error.
func (b *CompactableBuffer) Update(address *EntryAddress, data []byte) error {
	address.LockForWrite()
	defer address.UnlockWrite()
	header, err := b.ReadHeader(address)
	if err != nil {
		return err
	}
	beforeUpdataDataSize := header.dataSize
	afterUpdateDataSize := len(data) + VarIntSize(len(data))
	dataSizeDelta := afterUpdateDataSize - int(beforeUpdataDataSize)

	remainingSpace := int(header.entrySize) - reservedSize - afterUpdateDataSize
	header.dataSize = int64(afterUpdateDataSize)
	if remainingSpace <= 0 {
		atomic.AddInt64(&b.dataSize, int64(-beforeUpdataDataSize))
		atomic.AddInt64(&b.entrySize, int64(-header.entrySize))
		return b.expand(address, data)
	}

	atomic.AddInt64(&b.dataSize, int64(dataSizeDelta))
	var target = make([]byte, 0)
	AppendToBytes(data, &target)
	if len(target) > int(header.dataSize) {
		return io.EOF
	}
	writableBuffer := b.writableBuffer()
	_, err = writableBuffer.Write(address.Position()+reservedSize, target...)
	return err
}

//Remove flags passed in address entry as deleted. It locks address for write.
func (b *CompactableBuffer) Remove(address *EntryAddress) error {
	address.LockForWrite()
	defer address.UnlockWrite()
	result := b.removeWithoutLock(address)
	return result
}

func (b *CompactableBuffer) removeWithoutLock(address *EntryAddress) error {
	header, err := b.ReadHeader(address)
	if err != nil {
		return err
	}
	atomic.AddInt64(&b.dataSize, -header.dataSize)
	atomic.AddInt64(&b.entrySize, -header.entrySize)
	atomic.AddInt64(&b.removedCount, 1)
	atomic.AddInt64(&b.count, -1)
	buffer, err := b.bufferForAddress(address)
	if err != nil {
		return err
	}
	return buffer.remove(address)
}

//ReadHeader reads data entry header for passed in address.
func (b *CompactableBuffer) ReadHeader(address *EntryAddress) (*EntryHeader, error) {
	readableBuffer, err := b.bufferForAddress(address)
	if err != nil {
		return nil, fmt.Errorf("Failed ReadHeader, unable match buffer with address buffer id %v %v", address, err)
	}
	return readableBuffer.ReadHeader(address)
}

//ReadEntry reads data entry for passed in address. It locks address for read.
func (b *CompactableBuffer) ReadEntry(address *EntryAddress) (*Entry, error) {
	address.LockForRead()
	defer address.UnlockRead()
	return b.ReadEntryWithoutLocking(address)
}

//ReadEntryWithoutLocking reads data entry for passed in address without address locking.
func (b *CompactableBuffer) ReadEntryWithoutLocking(address *EntryAddress) (*Entry, error) {
	readableBuffer, err := b.bufferForAddress(address)
	if err != nil {
		return nil, fmt.Errorf("Failed ReadHeader, unable match buffer with address buffer id %v %v", address, err)
	}
	return readableBuffer.ReadEntry(address)
}

//EntryReaderAt returns io.ReaderAt for passed in address. ReaderAt locks address for reading.
func (b *CompactableBuffer) EntryReaderAt(address *EntryAddress) (io.ReaderAt, error) {
	return newCompatableBufferReader(b, address), nil
}

//EntryReader returns io.ReaderAt for passed in address. Reader locks address for reading.
func (b *CompactableBuffer) EntryReader(address *EntryAddress) (io.Reader, error) {
	return newCompatableBufferReader(b, address), nil
}

func (b *CompactableBuffer) allocateEntry(data []byte) *Entry {
	writableBuffer := b.writableBuffer()
	header := &EntryHeader{status: statusValid}
	payloadSize := len(data)
	extensionSize := int(float32(payloadSize)*writableBuffer.config.ExtensionFactor) - payloadSize
	header.entrySize = int64(payloadSize + extensionSize + reservedSize)
	header.entrySize = header.entrySize + int64(VarIntSize(int(header.entrySize)))
	header.dataSize = int64(len(data))
	return &Entry{EntryHeader: header, data: &data}
}

func (b *CompactableBuffer) moveEntry(address *EntryAddress, target *AddressableBuffer) error {
	entry, err := b.ReadEntryWithoutLocking(address)
	if err != nil {
		return fmt.Errorf("Failed to move address %v to target buffer due to %v", address.Position(), err)
	}
	previousAddress := address.Clone()
	if entry.IsValid() {
		err := target.move(address, entry)
		if err != nil {
			return fmt.Errorf("Failed to move address %v unable to move %v", address.Position(), err)
		}
		err = b.removeWithoutLock(previousAddress)
		if err != nil {
			return fmt.Errorf("Failed to move address %v unable to removeWithoutLock %v", address.Position(), err)
		}
		atomic.AddInt64(&b.dataSize, entry.dataSize)
		atomic.AddInt64(&b.entrySize, entry.entrySize)
		atomic.AddInt64(&b.count, 1)
	}
	return nil
}

//Compact copies over all entry from current buffer to compacting buffer back and forth.
func (b *CompactableBuffer) Compact() error {
	sourceBuffer := b.writableBuffer()
	if sourceBuffer.Count() == 0 {
		return nil
	}
	var compatableConfig, err = getCompatableBufferConfig(b.config.BufferConfig)
	if err != nil {
		return fmt.Errorf("Failed compact %v", err)
	}
	compactingBuffer, err := NewAddressableBuffer(compatableConfig)
	if err != nil {
		return fmt.Errorf("Failed to compact buffer due to %v", err)
	}
	err = b.compactWithBuffer(compactingBuffer)
	return err
}

func (b *CompactableBuffer) compactWithBuffer(compactingBuffer *AddressableBuffer) error {
	sourceBuffer := b.readableBuffer()
	writablePointer := unsafe.Pointer(sourceBuffer)
	compactingPointer := unsafe.Pointer(compactingBuffer)

	atomic.StorePointer(&b.tempBufferPointer, compactingPointer)
	if !atomic.CompareAndSwapPointer(&b.writableBufferPointer, writablePointer, compactingPointer) {
		return errors.New("Failed to compact - only one compaction is allowed at a time.")
	}
	defer atomic.CompareAndSwapPointer(&b.writableBufferPointer, compactingPointer, writablePointer)
	err := b.compactBuffer(sourceBuffer, compactingBuffer)
	if err != nil {
		return fmt.Errorf("Failed to compact buffer due to %v", err)
	}
	err = sourceBuffer.reset()
	if err != nil {
		return fmt.Errorf("Failed to compact buffer due to %v", err)
	}
	err = b.compactBuffer(compactingBuffer, sourceBuffer)
	if err != nil {
		return fmt.Errorf("Failed to compact from compacting buffer due to %v", err)
	}
	atomic.StorePointer(&b.readableBufferPointer, writablePointer)
	atomic.StorePointer(&b.tempBufferPointer, unsafe.Pointer(emptyAddressableBuffer))

	err = compactingBuffer.Close()
	if err != nil {
		return fmt.Errorf("Failed to close compacting buffer due to %v", err)
	}
	err = os.Remove(compactingBuffer.config.Filename)
	if err != nil {
		return fmt.Errorf("Failed to compact buffer - unable to remove file %v", compactingBuffer.config.Filename)
	}
	return nil
}

func (b *CompactableBuffer) compactEntry(address *EntryAddress, target *AddressableBuffer) (bool, error) {
	address.LockForWrite()
	defer address.UnlockWrite()

	if address.BufferId() == target.config.BufferId {
		return false, nil //already compacted
	}
	header, err := b.ReadHeader(address)
	if err != nil {
		return false, err
	}
	if !header.IsValid() {
		return false, nil
	}

	err = b.moveEntry(address, target)
	if err != nil {
		return false, fmt.Errorf("Failed to compact buffer due to %v", err)
	}
	return true, nil
}

func (b *CompactableBuffer) compactBuffer(source, target *AddressableBuffer) error {
	for {
		addresses := source.Addresses()
		moved := 0
		for _, address := range addresses {
			isMoved, err := b.compactEntry(address, target)
			if err != nil {
				return err
			}
			if isMoved {
				moved++
			}
		}

		if moved == 0 && source.resetAddresses(len(addresses)) {
			atomic.AddInt64(&b.removedCount, -source.removedCount)
			atomic.StoreInt64(&source.removedCount, 0)
			break
		}
	}
	return nil
}

func (b *CompactableBuffer) runAutoCompactionIfNeeded() {
	if b.config.AutoCompactionPolicy == CompactionSpaceBased {
		writableBuffer := b.writableBuffer()
		if writableBuffer.RemainingSpacePct() >= b.config.RequiredFreeSpacetPct {
			return
		}
		if b.DeletionPct() < b.config.DeletionPctThreshold {
			return
		}
	}
	err := b.Compact()
	if err != nil {
		fmt.Printf("Failed to compact %v", err)
	}
}

func (b *CompactableBuffer) manageAutoCompaction() {
	if b.config.AutoCompactionPolicy == CompactionDisabled {
		return
	}
	go func() {
		atomic.StoreInt32(&b.autoCompactionEnabled, 1)
		b.compactionWaitGroup.Add(1)
		defer b.compactionWaitGroup.Done()
		var cycleStartTime = time.Now()

		for {
			if atomic.LoadInt32(&b.autoCompactionEnabled) == int32(0) {
				return
			}

			var currentTime = time.Now()
			elapsedInSec := currentTime.Unix() - cycleStartTime.Unix()
			cycleStartTime = currentTime
			remainingTime := b.config.FrequencyInSec - int(elapsedInSec)
			select {
			case <-b.notification:
				break
			case <-time.After(time.Duration(remainingTime) * time.Second):
				b.runAutoCompactionIfNeeded()
			}

		}

	}()
}

func newAddressableBuffer(config *BufferConfig) (*AddressableBuffer, error) {
	var fileExists = false
	if _, err := os.Stat(config.Filename); err == nil {
		fileExists = true
	}
	var addressableBuffer, err = NewAddressableBuffer(config)
	if err != nil {
		return nil, err
	}

	if fileExists {
		err := addressableBuffer.load()
		if err != nil {
			return nil, fmt.Errorf("Failed to load buffer %v", err)
		}
	}
	return addressableBuffer, nil
}

func getCompatableBufferConfig(config *BufferConfig) (*BufferConfig, error) {
	fileInfo, err := os.Stat(config.Filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to get compatible buffer - unable to check fileinfo: %v", err)
	}
	return &BufferConfig{
		BufferId:        compactionBase + config.BufferId,
		Filename:        config.Filename + "c",
		InitialSize:     int(fileInfo.Size()),
		ExtensionFactor: config.ExtensionFactor,
	}, nil
}

func loadCompactingBufferIdNeeded(config *BufferConfig) (*AddressableBuffer, error) {
	var compatableConfig, err = getCompatableBufferConfig(config)
	if err != nil {
		return nil, fmt.Errorf("Failed to load compatible buffer %v", err)
	}

	if _, err := os.Stat(compatableConfig.Filename); os.IsNotExist(err) {
		return nil, nil
	}

	buffer, err := newAddressableBuffer(compatableConfig)
	if err != nil {
		return nil, err
	}
	if buffer.config.BufferId != config.BufferId+compactionBase {
		return nil, fmt.Errorf("Invalid compating buffer id expected: %v, but had: %v", config.BufferId+compactionBase, buffer.config.BufferId)
	}
	return buffer, nil

}

//NewCompactableBuffer returns a new *CompactableBuffer or error. It takes *CompatbleBufferConfig as parameter.
func NewCompactableBuffer(config *CompatbleBufferConfig) (*CompactableBuffer, error) {
	addressableBuffer, err := newAddressableBuffer(config.BufferConfig)
	if err != nil {
		return nil, fmt.Errorf("Failed to create compatable buffer - unable to create addressable buffer %v", err)
	}
	compatableBuffer, err := loadCompactingBufferIdNeeded(config.BufferConfig)
	if err != nil {
		return nil, fmt.Errorf("Failed to create compatable buffer,  %v", err)
	}

	result := &CompactableBuffer{config: config}
	result.setReadableBuffer(addressableBuffer)
	result.setWritableBuffer(addressableBuffer)

	if compatableBuffer != nil && compatableBuffer.Count() > 0 {
		err = result.compactWithBuffer(compatableBuffer)
		if err != nil {
			return nil, fmt.Errorf("Failed to create buffer - unable compact %v", err)
		}
	}

	result.count = int64(addressableBuffer.Count())
	result.removedCount = addressableBuffer.removedCount
	result.dataSize = addressableBuffer.dataSize
	result.entrySize = addressableBuffer.entrySize
	result.compactionWaitGroup = &sync.WaitGroup{}
	result.notification = make(chan bool, 1)
	result.manageAutoCompaction()

	return result, nil
}
