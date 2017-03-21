package mmcb_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/mmcb"
	"github.com/viant/toolbox"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
)

type IntFoo struct {
	Id   int
	Name string
}

func(f *IntFoo) GetId() int {
	return f.Id;
}

type StringFoo struct {
	Id   string
	Name string
}

func(f *StringFoo) GetId() string {
	return f.Id;
}


type IntFooMap struct {
	*mmcb.AbstractIntMap
}

func (m *IntFooMap) Put(key int, value *IntFoo) error {
	var data, err = m.Encode(value)
	if err != nil {
		return fmt.Errorf("Failed to put data %v", err)
	}
	address := m.GetAddress(key)
	if address == nil {
		address, err = m.Buffer.Add(data)
	} else {
		err = m.Buffer.Update(address, data)
	}
	if err != nil {
		fmt.Printf("Failed to modify data %v", err)
	}
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	m.Map[key] = address
	return nil
}

func (m *IntFooMap) Get(key int) (*IntFoo, error) {
	address := m.GetAddress(key)
	if address == nil {
		return nil, nil
	}
	reader, err := m.Buffer.EntryReader(address)
	if err != nil {
		return nil, fmt.Errorf("Failed to get data for %v - unable get reader %v", key, err)
	}
	var result = &IntFoo{}
	err = m.DecoderFactory.Create(reader).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("Failed to get data for %v - unable decode %v", key, err)
	}
	return result, nil
}

func (m *IntFooMap) Remove(key int) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if address, found := m.Map[key]; found {
		err := m.Buffer.Remove(address)
		if err != nil {
			return fmt.Errorf("Failed to remove key: %v, %v", key, err)
		}
		delete(m.Map, key)
	}
	return nil
}

func (m *IntFooMap) loadPersistedData() error {
	addresses, err := m.Buffer.Addresses()
	if err != nil {
		return fmt.Errorf("Failed to load persisted data - unable to get addresses %v", err)
	}
	for _, address := range addresses {
		reader, err := m.Buffer.EntryReader(address)
		if err != nil {
			return fmt.Errorf("Failed to get  EntryReader %v", err)
		}
		foo := &IntFoo{}
		err = m.Decode(reader, foo)
		if err != nil {
			return fmt.Errorf("Failed to load persisted data %v", err)
		}
		m.Map[foo.Id] = address
	}
	return nil
}

func NewIntFooMap(encoderFactory toolbox.EncoderFactory, decoderFactory toolbox.DecoderFactory, config *mmcb.CompatbleBufferConfig) (*IntFooMap, error) {
	abstractIntMap, err := mmcb.NewAbstractIntMap(config, encoderFactory, decoderFactory)
	if err != nil {
		return nil, fmt.Errorf("Failed to create a foo map %v", err)
	}
	result := &IntFooMap{abstractIntMap}
	err = result.loadPersistedData()
	if err != nil {
		return nil, fmt.Errorf("Failed to create a foo map %v", err)
	}
	return result, nil
}

func TestIntFooMap(t *testing.T) {
	filenames := []string{"/tmp/intfoo.mmf", "/tmp/intfoo.mmfc"}
	err := removeTestFiles(filenames)
	config, err := mmcb.NewCompatbleBufferConfig(uint8(1), filenames[0], 8*1024, 1.2, mmcb.CompactionSpaceBased, 70, 20.0, 1)
	assert.Nil(t, err)
	aMap, err := NewIntFooMap(toolbox.NewJSONEncoderFactory(), toolbox.NewJSONDecoderFactory(), config)
	assert.Nil(t, err)
	err = aMap.Put(1, &IntFoo{101, "abc"})
	assert.Nil(t, err)
	foo, err := aMap.Get(1)
	assert.Nil(t, err)
	assert.Equal(t, 101, foo.Id)
	assert.Equal(t, "abc", foo.Name)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(100000)
	for i := 0; i < 100000; i++ {
		go func(i int) {
			foo := &IntFoo{Id: i % 1000, Name: fmt.Sprintf("abc%v%v", i%1000, rand.Int())}
			err := aMap.Put(i%1000, foo)
			waitGroup.Done()
			assert.Nil(t, err)
		}(i)
	}
	waitGroup.Wait()

	assert.Equal(t, 1000, aMap.Size())

	err = aMap.Remove(0)
	assert.Nil(t, err)
	assert.Equal(t, 999, aMap.Size())

	for i := 1; i < 1000; i++ {
		foo, err := aMap.Get(i)
		assert.Nil(t, err)
		assert.Equal(t, i, foo.Id)
		assert.True(t, strings.Contains(foo.Name, fmt.Sprintf("abc%v", i%1000)))
	}

	err = aMap.Close()
	assert.Nil(t, err)
	aMap, err = NewIntFooMap(toolbox.NewJSONEncoderFactory(), toolbox.NewJSONDecoderFactory(), config)
	assert.Nil(t, err)
	assert.Equal(t, 999, aMap.Size())
	assert.Equal(t, 999, len(aMap.Keys()))

	//for i := 1; i < 1000; i++ {
	//	foo, err := aMap.Get(i)
	//	assert.Nil(t, err)
	//	assert.Equal(t, i, foo.Id)
	//	assert.True(t, strings.Contains(foo.Name, fmt.Sprintf("abc%v", i%1000)))
	//}
	//
	err = aMap.Close()
	assert.Nil(t, err)
	err = removeTestFiles(filenames)
	assert.Nil(t, err)

}

type StringFooMap struct {
	*mmcb.AbstractStringMap
}

func (m *StringFooMap) Put(key string, value *StringFoo) error {
	var data, err = m.Encode(value)
	if err != nil {
		return fmt.Errorf("Failed to put data %v", err)
	}
	address := m.GetAddress(key)
	if address == nil {
		address, err = m.Buffer.Add(data)
	} else {
		err = m.Buffer.Update(address, data)
	}
	if err != nil {
		fmt.Printf("Failed to modify data %v", err)
	}
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	m.Map[key] = address
	return nil
}

func (m *StringFooMap) Get(key string) (*StringFoo, error) {
	address := m.GetAddress(key)
	if address == nil {
		return nil, nil
	}
	reader, err := m.Buffer.EntryReader(address)
	if err != nil {
		return nil, fmt.Errorf("Failed to get data for %v - unable get reader %v", key, err)
	}
	var result = &StringFoo{}
	err = m.DecoderFactory.Create(reader).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("Failed to get data for %v - unable decode %v", key, err)
	}
	return result, nil
}

func (m *StringFooMap) Remove(key string) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if address, found := m.Map[key]; found {
		err := m.Buffer.Remove(address)
		if err != nil {
			return fmt.Errorf("Failed to remove key: %v, %v", key, err)
		}
		delete(m.Map, key)
	}
	return nil
}

func (m *StringFooMap) loadPersistedData() error {
	addresses, err := m.Buffer.Addresses()
	if err != nil {
		return fmt.Errorf("Failed to load persisted data - unable to get addresses %v", err)
	}
	for _, address := range addresses {
		reader, err := m.Buffer.EntryReader(address)
		if err != nil {
			return fmt.Errorf("Failed to get  EntryReader %v", err)
		}
		foo := &StringFoo{}
		err = m.Decode(reader, foo)
		if err != nil {
			return fmt.Errorf("Failed to load persisted data %v", err)
		}
		m.Map[foo.Name] = address
	}
	return nil
}

func NewStringFooMap(encoderFactory toolbox.EncoderFactory, decoderFactory toolbox.DecoderFactory, config *mmcb.CompatbleBufferConfig) (*StringFooMap, error) {
	abstractStringMap, err := mmcb.NewAbstractStringMap(config, encoderFactory, decoderFactory)
	if err != nil {
		return nil, fmt.Errorf("Failed to create a foo map %v", err)
	}
	result := &StringFooMap{abstractStringMap}
	err = result.loadPersistedData()
	if err != nil {
		return nil, fmt.Errorf("Failed to create a foo map %v", err)
	}
	return result, nil
}



func TestStringFooMap(t *testing.T) {
	filenames := []string{"/tmp/stringfoo.mmf", "/tmp/stringfoo.mmfc"}
	err := removeTestFiles(filenames)
	config, err := mmcb.NewCompatbleBufferConfig(uint8(1), filenames[0], 8*1024, 1.2, mmcb.CompactionSpaceBased, 70, 20.0, 1)
	assert.Nil(t, err)
	aMap, err := NewStringFooMap(toolbox.NewJSONEncoderFactory(), toolbox.NewJSONDecoderFactory(), config)
	assert.Nil(t, err)
	err = aMap.Put("aaa", &StringFoo{"101", "abc"})
	assert.Nil(t, err)

	foo, err := aMap.Get("aaac")
	assert.Nil(t, err)
	assert.Nil(t, foo)

	foo, err = aMap.Get("aaa")
	assert.Nil(t, err)
	assert.Equal(t, "101", foo.Id)
	assert.Equal(t, "abc", foo.Name)

	err = aMap.Close()
	assert.Nil(t, err)
	aMap, err = NewStringFooMap(toolbox.NewJSONEncoderFactory(), toolbox.NewJSONDecoderFactory(), config)
	assert.Nil(t, err)
	assert.Equal(t, 1, aMap.Size())
	keys := aMap.Keys()

	assert.Equal(t, 1, len(keys))

	err = aMap.Remove("aaa")
	assert.Nil(t, err)

	foo, err = aMap.Get("aaa")
	assert.Nil(t, err)
	assert.Nil(t, foo)

	err = aMap.Close()
	assert.Nil(t, err)
	err = removeTestFiles(filenames)
	assert.Nil(t, err)

}

func removeTestFiles(filenames []string) error {
	for _, filename := range filenames {
		if _, err := os.Stat(filename); err != nil {
			continue
		}

		err := os.Remove(filename)
		if err != nil {
			return err
		}
	}
	return nil
}



func TestNewAbstractMap(t *testing.T) {

	{
		filenames := []string{"/tmp/iabsfoo.mmf", "/tmp/iabsfoo.mmfc"}
		err := removeTestFiles(filenames)

		config, err := mmcb.NewCompatbleBufferConfig(uint8(1), filenames[0], 8 * 1024, 1.2, mmcb.CompactionSpaceBased, 70, 20.0, 1)
		assert.Nil(t, err)
		valurProvider := func() interface{} {
			return &IntFoo{}
		}

		abstractMap, err := mmcb.NewAbstractMap(nil, nil, config, valurProvider, true, "");
		assert.Nil(t, err)
		assert.False(t, abstractMap.Exists(1));
		err = abstractMap.Put(&IntFoo{1, "Test 1"});
		assert.Nil(t, err)
		assert.True(t, abstractMap.Exists(1));

		err = abstractMap.Put(&IntFoo{2, "Test 2"});
		assert.Nil(t, err)
		assert.True(t, abstractMap.Exists(2));
		err = abstractMap.Remove(1);
		assert.False(t, abstractMap.Exists(1));
		abstractMap.Close();
		abstractMap, err = mmcb.NewAbstractMap(nil, nil, config, valurProvider, true, "");
		assert.True(t, abstractMap.Exists(2));
		abstractMap.Close();
		err = removeTestFiles(filenames)
		assert.Nil(t, err)

	}

	{
		filenames := []string{"/tmp/iabsfoo.mmf", "/tmp/iabsfoo.mmfc"}
		err := removeTestFiles(filenames)

		config, err := mmcb.NewCompatbleBufferConfig(uint8(1), filenames[0], 8 * 1024, 1.2, mmcb.CompactionSpaceBased, 70, 20.0, 1)
		assert.Nil(t, err)
		valurProvider := func() interface{} {
			return &IntFoo{}
		}

		abstractMap, err := mmcb.NewAbstractMap(nil, nil, config, valurProvider, true, "");
		assert.Nil(t, err)
		assert.False(t, abstractMap.Exists(1));
		err = abstractMap.Put(&IntFoo{1, "Test 1"});
		assert.Nil(t, err)
		assert.True(t, abstractMap.Exists(1));

		err = abstractMap.Put(&IntFoo{2, "Test 2"});
		assert.Nil(t, err)
		assert.True(t, abstractMap.Exists(2));
		err = abstractMap.Remove(1);
		assert.False(t, abstractMap.Exists(1));
		abstractMap.Close();
		abstractMap, err = mmcb.NewAbstractMap(nil, nil, config, valurProvider, true, "");
		assert.True(t, abstractMap.Exists(2));
		abstractMap.Close();
		err = removeTestFiles(filenames)
		assert.Nil(t, err)

	}
	{
		filenames := []string{"/tmp/sabsfoo.mmf", "/tmp/sabsfoo.mmfc"}
		err := removeTestFiles(filenames)

		config, err := mmcb.NewCompatbleBufferConfig(uint8(1), filenames[0], 8 * 1024, 1.2, mmcb.CompactionSpaceBased, 70, 20.0, 1)
		assert.Nil(t, err)
		valurProvider := func() interface{} {
			return &StringFoo{}
		}

		abstractMap, err := mmcb.NewAbstractMap(nil, nil, config, valurProvider, false, "");
		assert.Nil(t, err)
		assert.False(t, abstractMap.Exists("1"));
		err = abstractMap.Put(&StringFoo{"1", "Test 1"});
		assert.Nil(t, err)
		assert.True(t, abstractMap.Exists("1"));

		err = abstractMap.Put(&StringFoo{"2", "Test 2"});
		assert.Nil(t, err)
		assert.True(t, abstractMap.Exists("2"));
		err = abstractMap.Remove("1");
		assert.False(t, abstractMap.Exists("1"));
		abstractMap.Close();
		abstractMap, err = mmcb.NewAbstractMap(nil, nil, config, valurProvider, false, "");
		assert.Nil(t, err)

		assert.True(t, abstractMap.Exists("2"));
		abstractMap.Close();
		err = removeTestFiles(filenames)
		assert.Nil(t, err)

	}

}