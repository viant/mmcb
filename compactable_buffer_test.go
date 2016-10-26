package mmcb_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/mmcb"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestCompatableBuffer(t *testing.T) {

	filename := "/tmp/compatableBuffer.mmf"
	var id = uint8(1)
	err := os.Remove(filename)
	config, err := mmcb.NewCompatbleBufferConfig(id, "/tmp/compatableBuffer.mmf", 1024*32, 1.2, 0, 0.0, 0.0, 0)
	if err != nil {
		panic(err.Error())
	}
	buffer, err := mmcb.NewCompactableBuffer(config)
	address1, err := buffer.Add([]byte("abcdeflors"))
	assert.Nil(t, err)

	address2, err := buffer.Add([]byte("xyz1024zyx131213123123213"))
	assert.Nil(t, err)

	entry1, err := buffer.ReadEntry(address1)
	assert.Nil(t, err)
	assert.Equal(t, "abcdeflors", string(entry1.Data()))

	{
		reader, err := buffer.EntryReader(address1)
		assert.Nil(t, err)
		var out = make([]byte, 10)
		bytesRead, err := reader.Read(out)
		assert.Equal(t, 10, bytesRead)
		assert.Equal(t, "abcdeflors", string(out))

		bytesRead, err = reader.Read(out)
		assert.Equal(t, 0, bytesRead)
		assert.Equal(t, io.EOF, err)
	}

	entry2, err := buffer.ReadEntry(address2)
	assert.Nil(t, err)
	assert.Equal(t, "xyz1024zyx131213123123213", string(entry2.Data()))

	//test entry from invalid address
	_, err = buffer.ReadEntry(mmcb.NewEntryAddress(uint8(4), 1))
	assert.NotNil(t, err)

	{
		//Test update - size fits
		address1Clone := address1.Clone()
		err := buffer.Update(address1, []byte("abcdeflors1"))
		assert.Nil(t, err)
		assert.Equal(t, address1Clone.Position(), address1.Position())

		entry1, err := buffer.ReadEntry(address1)
		assert.Nil(t, err)
		assert.Equal(t, "abcdeflors1", string(entry1.Data()))
	}
	{ //update to bigger size
		//Test update - does not fits the existing size
		address1Clone := address1.Clone()
		err := buffer.Update(address1, []byte("abcdeflors1234"))
		assert.Nil(t, err)
		assert.NotEqual(t, address1Clone.Position(), address1.Position())
		assert.Equal(t, int16(0), address1.ReadCounter())

		headerOriginal, err := buffer.ReadHeader(address1Clone)
		assert.Nil(t, err)
		assert.True(t, headerOriginal.IsRemoved())
		entry1, err := buffer.ReadEntry(address1)
		assert.Nil(t, err)
		assert.Equal(t, "abcdeflors1234", string(entry1.Data()))
	}

	assert.Equal(t, int16(0), address1.ReadCounter())
	{

		//update to smaller size
		err := buffer.Update(address1, []byte("avc"))
		assert.Nil(t, err)

	}

	assert.Equal(t, 2, buffer.Count())
	entries, err := buffer.Addresses()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(entries))
	assert.Equal(t, 1, buffer.RemovedCount())

}

//This test focuss on high contetion per one address (each address will be updated 10000 times
func TestParallelWrites(t *testing.T) {
	filename := "/tmp/compatable_parallel.mmf"
	err := os.Remove(filename)
	config, err := mmcb.NewCompatbleBufferConfig(uint8(1), filename, 1024*32, 1.2, 0, 0.0, 0.0, 0)
	if err != nil {
		panic(err.Error())
	}
	buffer, err := mmcb.NewCompactableBuffer(config)
	assert.Nil(t, err)
	addresses := make([]*mmcb.EntryAddress, 10)
	for i := 0; i < 10; i++ {
		address, err := buffer.Add([]byte("abcdefghijklmnop" + strconv.Itoa(i)))
		assert.Nil(t, err)
		addresses[i] = address
	}

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(100000)
	for j := 0; j < 100000; j++ {
		i := j % 10
		if addresses[i] == nil {
			panic(fmt.Sprintf("Address %v was nil", i))

		}
		go func(address *mmcb.EntryAddress, data []byte) {
			err := buffer.Update(address, data)
			if err != nil {
				panic(err.Error())
			}
			waitGroup.Done()
		}(addresses[i], []byte("abcdefghijklm"+strconv.Itoa(j)+strconv.Itoa(i)))
	}
	waitGroup.Wait()
	//verifies that scn has changed to 1000
	for i := 0; i < 10; i++ {
		assert.Equal(t, int(addresses[i].ReadCounter()), int(0))
	}

}

func TestSimpleCompaction(t *testing.T) {
	var id = uint8(1)

	filesNames := []string{
		"/tmp/entrybuffer.mmf",
		"/tmp/entrybuffer.mmfc",
	}
	err := removeTestFiles(filesNames)

	config, err := mmcb.NewCompatbleBufferConfig(id, filesNames[0], 1024*64, 1.2, 0, 0.0, 0.0, 0)
	if err != nil {
		panic(err.Error())
	}
	buffer, err := mmcb.NewCompactableBuffer(config)
	if err != nil {
		panic(err.Error())
	}
	populateBuffer(t, buffer)

	{
		//unidirectional compaction
		assert.Equal(t, 750, buffer.Count())
		assert.Equal(t, 250, buffer.RemovedCount())
		err = buffer.Compact()
		if err != nil {
			panic(err.Error())
		}
		assert.Equal(t, 750, buffer.Count())
		assert.Equal(t, 0, buffer.RemovedCount())

	}
	err = removeTestFiles(filesNames)
	assert.Nil(t, err)

}

func TestSimpleLoadingDataFromCompactableBuffer(t *testing.T) {
	var err error
	var id = uint8(1)
	filenames := []string{
		"/tmp/entrybuffer.mmf",
		"/tmp/entrybuffer.mmfc",
	}

	for _, filename := range filenames {
		err = os.Remove(filename)
	}

	config, err := mmcb.NewCompatbleBufferConfig(uint8(64)+id, filenames[0], 1024*64, 1.2, 0, 0.0, 0.0, 0)
	if err != nil {
		panic(err.Error())
	}
	buffer, err := mmcb.NewCompactableBuffer(config)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		_, err := buffer.Add([]byte("abcdef" + strconv.Itoa(i) + "xyz"))
		assert.Nil(t, err)
	}
	assert.Equal(t, 100, buffer.Count())
	err = buffer.Close()
	assert.Nil(t, err)
	err = os.Rename(filenames[0], filenames[1])
	assert.Nil(t, err)

	{ //test invalid buffer id error
		config, err := mmcb.NewCompatbleBufferConfig(uint8(3), filenames[0], 1024*64, 1.2, 0, 0.0, 0.0, 0)
		if err != nil {
			panic(err.Error())
		}
		_, err = mmcb.NewCompactableBuffer(config)
		assert.NotNil(t, err)
	}

	err = os.Remove(filenames[0])
	assert.Nil(t, err)

	config, err = mmcb.NewCompatbleBufferConfig(id, filenames[0], 1024*64, 1.2, 0, 0.0, 0.0, 0)
	if err != nil {
		panic(err.Error())
	}
	buffer, err = mmcb.NewCompactableBuffer(config)
	assert.Nil(t, err)
	assert.Equal(t, 100, buffer.Count())

}

func populateBuffer(t *testing.T, buffer *mmcb.CompactableBuffer) []*mmcb.EntryAddress {
	var addresses = make([]*mmcb.EntryAddress, 0)
	for i := 0; i < 1000; i++ {
		address, err := buffer.Add([]byte("abcdef" + strconv.Itoa(i) + "xyz"))
		if err != nil {
			panic(err.Error())
		}
		addresses = append(addresses, address)

	}
	assert.Equal(t, 22890, buffer.EntrySize())
	assert.Equal(t, 11890, buffer.DataSize())

	for i := 1; i < 1000; i += 5 {
		err := buffer.Update(addresses[i], []byte("a"+strconv.Itoa(i)+"xyz"))
		if err != nil {
			panic(err.Error())
		}
	}

	assert.Equal(t, 11090, buffer.DataSize())
	assert.Equal(t, 22890, buffer.EntrySize())

	for i := 0; i < 1000; i++ {
		if i%4 == 0 {
			err := buffer.Remove(addresses[i])
			if err != nil {
				panic(err.Error())
			}
		}
	}

	assert.Equal(t, 8368, buffer.DataSize())
	assert.Equal(t, 17168, buffer.EntrySize())

	return addresses
}

func TestCompactionWithParalelWrites(t *testing.T) {
	for z := 0; z < 10; z++ {
		var id = uint8(1)
		filenames := []string{
			"/tmp/entrybuffer.mmf",
			"/tmp/entrybuffer.mmfc",
		}

		err := removeTestFiles(filenames)
		config, err := mmcb.NewCompatbleBufferConfig(id, filenames[0], 1024*64, 1.2, 0, 0.0, 0.0, 0)
		if err != nil {
			panic(err.Error())
		}
		buffer, err := mmcb.NewCompactableBuffer(config)
		if err != nil {
			panic(err.Error())
		}
		addresses := populateBuffer(t, buffer)
		waitGroup := &sync.WaitGroup{}
		waitGroup.Add(100000)
		for j := 0; j < 100000; j++ {
			i := j % 1000
			go func(index int, address *mmcb.EntryAddress, data []byte) {
				defer waitGroup.Done()
				if !(index%4 == 0) {
					err := buffer.Update(address, data)
					if err != nil {
						panic(err.Error())
					}
				}

				if index == 1000 {
					err = buffer.Compact()
					if err != nil {
						panic(err.Error())
					}
				}
			}(j, addresses[i], []byte("abcdefgabcdefghijklm"+strconv.Itoa(j)+strconv.Itoa(i)))
		}
		waitGroup.Wait()
		err = buffer.Close()
		assert.Nil(t, err)
		err = removeTestFiles(filenames)
		assert.Nil(t, err)
	}

}

func TestAutoCompatableBuffer(t *testing.T) {
	var filenames = []string{
		"/tmp/auto_compact.mmf",
		"/tmp/auto_compact.mmfc",
	}

	err := removeTestFiles(filenames)
	config, err := mmcb.NewCompatbleBufferConfig(uint8(1), filenames[0], 8*1024, 1.2, mmcb.CompactionSpaceBased, 10, 90.0, 1)
	buffer, err := mmcb.NewCompactableBuffer(config)
	if err != nil {
		panic(err.Error())
	}
	assert.Nil(t, err)

	addresses := make([]*mmcb.EntryAddress, 200)
	for i := 0; i < 100; i++ {
		addresses[i], err = buffer.Add([]byte("a" + strconv.Itoa(i) + "xyz"))
		assert.Nil(t, err)
	}

	for i := 0; i < 100; i++ {
		if i%4 == 0 {
			err := buffer.Remove(addresses[i])
			assert.Nil(t, err)
		}
	}
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		if i%4 == 0 {
			continue
		}
		entry, err := buffer.ReadEntry(addresses[i])
		assert.Nil(t, err, "should read from address["+strconv.Itoa(i)+"]")
		if err != nil {
			for k := 0; k < 10; k++ {
				fmt.Printf("\tAddress: %v => %v\n", k, addresses[k])

			}
		}

		assert.Equal(t, "a"+strconv.Itoa(i)+"xyz", string(entry.Data()))
	}
	for i := 100; i < 200; i++ {
		if i%5 == 0 {
			err := buffer.Update(addresses[i%100], []byte("abcdefghijk"+strconv.Itoa(i%100)+"xyz"))
			assert.Nil(t, err)
		}
		addresses[i], err = buffer.Add([]byte("a" + strconv.Itoa(i) + "xyz"))
		assert.Nil(t, err)
	}
	for i := 0; i < 200; i++ {
		if i%4 == 0 {
			continue
		}

		entry, err := buffer.ReadEntry(addresses[i])
		assert.Nil(t, err)
		expected := []byte("a" + strconv.Itoa(i) + "xyz")
		if i < 100 && i%5 == 0 {
			expected = []byte("abcdefghijk" + strconv.Itoa(i) + "xyz")
		}
		assert.Equal(t, string(expected), string(entry.Data()))
	}
	time.Sleep(2 * time.Second)
	assert.True(t, buffer.DeletionPct() < 10.0, fmt.Sprintf("should be less than 10 but had %v", buffer.DeletionPct()))
	err = buffer.Close()
	assert.Nil(t, err)

}
