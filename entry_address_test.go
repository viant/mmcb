package mmcb_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/mmcb"
	"sync"
	"testing"
	"time"
)

func TestEntryAddress(t *testing.T) {

	{
		address := mmcb.NewEntryAddress(uint8(123), uint64(0xff34ff))
		assert.Equal(t, uint8(123), address.BufferId())
		assert.Equal(t, int16(0), address.ReadCounter())
		assert.Equal(t, uint64(0xff34ff), address.Position())

	}

	//test max ranges - 1
	{
		var position = uint64(0xFFFFFFFFFF - 1)
		var bufferId = uint8(0xFF - 1)

		address := mmcb.NewEntryAddress(bufferId, position)
		assert.Equal(t, bufferId, address.BufferId())
		assert.Equal(t, position, address.Position())

	}

	//test max ranges
	{
		var position = uint64(0xFFFFFFFFFF)
		var bufferId = uint8(0xFF)
		address := mmcb.NewEntryAddress(bufferId, position)
		assert.Equal(t, bufferId, address.BufferId())
		assert.Equal(t, position, address.Position())

	}

	//test min ranges
	{
		var position = uint64(0x0)
		var bufferId = uint8(0x0)
		address := mmcb.NewEntryAddress(bufferId, position)
		assert.Equal(t, bufferId, address.BufferId())
		assert.Equal(t, position, address.Position())

	}

	//test min ranges
	{
		var position = uint64(0x0)
		var bufferId = uint8(0x1)
		address := mmcb.NewEntryAddress(bufferId, position)
		assert.Equal(t, bufferId, address.BufferId())
		assert.Equal(t, position, address.Position())

	}

}

func TestEntryAddressReadWriteLocking(t *testing.T) {
	address := mmcb.NewEntryAddress(uint8(1), uint64(0xff34ff))
	address.LockForRead()
	assert.Equal(t, true, address.IsReadLocked())

	address.LockForRead()
	assert.Equal(t, int16(2), address.ReadCounter())
	assert.True(t, address.IsReadLocked())

	waitGoup := sync.WaitGroup{}
	waitGoup.Add(2)

	go func() {
		address.LockForWrite()
		assert.True(t, address.IsWriteLocked())
		assert.Equal(t, int16(0), address.ReadCounter())
		waitGoup.Done()
		address.UnlockWrite()
	}()

	time.Sleep(100 * time.Millisecond)
	go func() {
		address.LockForRead()
		waitGoup.Done()
		assert.False(t, address.IsWriteLocked())
		assert.Equal(t, int16(1), address.ReadCounter())
		address.UnlockRead()
		assert.Equal(t, int16(0), address.ReadCounter())
	}()

	time.Sleep(100 * time.Millisecond)
	assert.True(t, address.IsWriteLocked())
	assert.Equal(t, int16(2), address.ReadCounter())
	address.UnlockRead()
	assert.True(t, address.IsWriteLocked())
	assert.Equal(t, int16(1), address.ReadCounter())
	address.UnlockRead()
	assert.Equal(t, int16(0), address.ReadCounter())

	waitGoup.Wait()
	time.Sleep(100 * time.Millisecond)
	assert.False(t, address.IsWriteLocked())
	assert.Equal(t, int16(0), address.ReadCounter())

}
