package mmcb_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/mmcb"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestAddressableBuffer(t *testing.T) {
	err := os.Remove("/tmp/test.mmf")

	config := &mmcb.BufferConfig{
		BufferId:        uint8(1),
		Filename:        "/tmp/test.mmf",
		InitialSize:     1024 * 32,
		ExtensionFactor: 1.2,
	}
	buffer, err := mmcb.NewAddressableBuffer(config)
	assert.Nil(t, err)
	if err != nil {
		t.FailNow()
	}
	assert.NotNil(t, buffer)

	var maxRoutines = 1024
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(maxRoutines)

	for i := 0; i < maxRoutines; i++ {
		go func(i int) {
			payload := []byte("abc" + strconv.Itoa(i%10))
			_, err = buffer.Write(uint64(i*128), payload...)
			assert.Nil(t, err)
			waitGroup.Done()

		}(i)

	}

	assert.Equal(t, float32(0.012207031), buffer.RemainingSpacePct())

	waitGroup.Wait()
	assert.Equal(t, 131072, buffer.FileSize())

	waitGroup.Add(maxRoutines)
	for i := 0; i < maxRoutines; i++ {
		go func(i int) {
			defer waitGroup.Done()
			expected := []byte("abc" + strconv.Itoa(i%10))
			var actual = make([]byte, 4)
			_, err := buffer.ReadAt(actual, uint64(i*128), 4)
			assert.Nil(t, err)
			assert.Equal(t, expected, actual)
		}(i)
	}
	waitGroup.Wait()

	err = buffer.Close()
	assert.Nil(t, err)
	err = os.Remove("/tmp/test.mmf")
	assert.Nil(t, err)

}
