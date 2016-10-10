package mmcb

import (
	"fmt"
	"sync/atomic"
	"time"
)

const addressLockShifts = 63
const addressSeqChangeNumberShifts = 64 - 16
const addressBufferIdShifts = 64 - 24
const addressPositionMask = 0xFFFFFFFFFF

const bufferIdMask = 0xFF

const lockMask = 0x1

const readCounterMask = 0x7FFF

const writeLocked = uint8(0x1)
const writeUnLocked = uint8(0x0)

//EntryAddress represents entry address. Encoded address uses uint64 to stores the following info:
//write lock status - the most significant bit, read lock counter 15 bits, 8 bites for buffer address,
// and 40 bites for position within the buffer.
type EntryAddress struct {
	encodedAddress uint64
}

//String returns address info
func (a *EntryAddress) String() string {
	return fmt.Sprintf("buffer:%v, readCounter: %v, position:%v isLocked %v,  %v", a.BufferId(), a.ReadCounter(), a.Position(), a.IsWriteLocked(), a.encodedAddress)
}
func (a *EntryAddress) readSafely() uint64 {
	return atomic.LoadUint64(&a.encodedAddress)
}

//BufferId returns buffer id associated with address.
func (a *EntryAddress) BufferId() uint8 {
	encodedAddress := a.readSafely()
	return uint8((encodedAddress >> addressBufferIdShifts) & bufferIdMask)
}

//ReadCounter returns reading lock counter.
func (a *EntryAddress) ReadCounter() int16 {
	encodedAddress := a.readSafely()
	return int16((encodedAddress >> addressSeqChangeNumberShifts) & readCounterMask)
}

//Position returns a position with buffer identified by buffer id.
func (a *EntryAddress) Position() uint64 {
	encodedAddress := a.readSafely()
	return uint64(encodedAddress & addressPositionMask)
}

//IsWriteLocked returns true if address is write locked.
func (a *EntryAddress) IsWriteLocked() bool {
	encodedAddress := a.readSafely()
	result := uint8((encodedAddress >> addressLockShifts)) & lockMask
	return result == writeLocked
}

//IsReadLocked returns true if address is read locked.
func (a *EntryAddress) IsReadLocked() bool {
	return a.ReadCounter() > 0
}

//Clone clones the current address.
func (a *EntryAddress) Clone() *EntryAddress {
	encodedAddress := a.readSafely()
	return &EntryAddress{encodedAddress}
}

func (a *EntryAddress) lockForReadEncodedAddress() uint64 {
	var clone = a.Clone()
	return encodeLockAddress(clone.BufferId(), clone.ReadCounter()+1, writeUnLocked, clone.Position())
}

//LockForRead locks address for read or waits on write completes. Other reads are non blocking.
func (a *EntryAddress) LockForRead() {
	sleepDuration := time.Microsecond
begin:
	clone := a.Clone()
	if clone.IsWriteLocked() {
		time.Sleep(sleepDuration) //give go routine schedule time to breathe
		goto begin
	}
	lockForWriteEncodedAddress := clone.lockForReadEncodedAddress()
	if !atomic.CompareAndSwapUint64(&a.encodedAddress, clone.encodedAddress, lockForWriteEncodedAddress) {
		goto begin
	}
}

func (a *EntryAddress) unlockReadEncodedAddress() uint64 {
	var clone = a.Clone()
	lockBit := writeUnLocked
	if clone.IsWriteLocked() {
		lockBit = writeLocked
	}
	return encodeLockAddress(clone.BufferId(), clone.ReadCounter()-1, lockBit, clone.Position())
}

//UnlockRead unlocks read.
func (a *EntryAddress) UnlockRead() {
	sleepDuration := time.Microsecond
begin:
	clone := a.Clone()
	unLockReadEncodedAddress := a.unlockReadEncodedAddress()
	if !atomic.CompareAndSwapUint64(&a.encodedAddress, clone.encodedAddress, unLockReadEncodedAddress) {
		time.Sleep(sleepDuration) //give go routine schedule time to breathe
		goto begin
	}
}

func (a *EntryAddress) lockForWriteEncodedAddress() uint64 {
	var encodedAddress = a.readSafely()
	var lockBit = uint64(writeLocked)
	lockBit = lockBit << addressLockShifts
	return lockBit | encodedAddress
}

//LockForWrite locks this address for write or waits or other write/reads to complete. It blocks subsequent reads.
func (a *EntryAddress) LockForWrite() {
	sleepDuration := time.Microsecond
	for {
		clone := a.Clone()
		if clone.IsWriteLocked() {
			time.Sleep(sleepDuration) //give go routine schedule time to breathe
			continue
		}

		lockForWriteEncodedAddress := clone.lockForWriteEncodedAddress()
		if !atomic.CompareAndSwapUint64(&a.encodedAddress, clone.encodedAddress, lockForWriteEncodedAddress) {
			time.Sleep(sleepDuration) //give go routine schedule time to breathe
			continue
		}

		break
	}

	for {
		clone := a.Clone()
		if clone.IsReadLocked() {
			time.Sleep(sleepDuration) //give go routine schedule time to breathe
			continue
		}

		break
	}
}

func (a *EntryAddress) unlockWriteEncodedAddress() uint64 {
	clone := a.Clone()
	return encodeLockAddress(clone.BufferId(), clone.ReadCounter(), writeUnLocked, clone.Position())
}

//UnlockWrite unlocks write.
func (a *EntryAddress) UnlockWrite() {
	sleepDuration := time.Microsecond
begin:
	clone := a.Clone()
	unlockWriteEncodedAddress := a.unlockWriteEncodedAddress()
	if !atomic.CompareAndSwapUint64(&a.encodedAddress, clone.encodedAddress, unlockWriteEncodedAddress) {
		time.Sleep(sleepDuration) //give go routine schedule time to breathe
		goto begin
	}
}

func encodeAddress(bufferId uint8, seqChangeNumber int16, position uint64) uint64 {
	return encodeLockAddress(bufferId, seqChangeNumber, writeUnLocked, position)
}

func encodeLockAddress(bufferId uint8, seqChangeNumber int16, lock uint8, position uint64) uint64 {
	var bufferIdBits = uint64(bufferId & bufferIdMask)
	bufferIdBits = bufferIdBits << addressBufferIdShifts
	var seqChangeNumberBits = uint64(seqChangeNumber) & readCounterMask
	seqChangeNumberBits = seqChangeNumberBits << addressSeqChangeNumberShifts
	var lockBit = uint64(lock)
	lockBit = lockBit << addressLockShifts
	positionBits := uint64(position & addressPositionMask)
	return lockBit | bufferIdBits | seqChangeNumberBits | positionBits
}

//NewEntryAddress create a new entry address.
func NewEntryAddress(bufferId uint8, position uint64) *EntryAddress {
	return &EntryAddress{encodeAddress(bufferId, zeroReadCounter, position)}
}
