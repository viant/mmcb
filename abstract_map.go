package mmcb

import (
	"bytes"
	"fmt"
	"github.com/viant/toolbox"
	"io"
	"sync"
)

//AbstractIntMap represent int key based map of int and *EntryAddress
type AbstractIntMap struct {
	Buffer         *CompactableBuffer
	Map            map[int]*EntryAddress
	Mutex          *sync.Mutex
	EncoderFactory toolbox.EncoderFactory
	DecoderFactory toolbox.DecoderFactory
}

//Keys returns a map key
func (m *AbstractIntMap) Keys() []int {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	var keys = make([]int, 0)
	for k := range m.Map {
		keys = append(keys, k)
	}
	return keys
}

//Size returns a map size
func (m *AbstractIntMap) Size() int {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return len(m.Map)
}

//Close closes underlying map
func (m *AbstractIntMap) Close() error {
	return m.Buffer.Close()
}

//GetAddress returns an address for the passed in key
func (m *AbstractIntMap) GetAddress(key int) *EntryAddress {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return m.Map[key]
}

//Encode encodes value into bytes or returns error
func (m *AbstractIntMap) Encode(value interface{}) ([]byte, error) {
	var data = new(bytes.Buffer)
	err := m.EncoderFactory.Create(data).Encode(value)
	if err != nil {
		return nil, fmt.Errorf("Failed to create encoder %v", err)
	}
	return data.Bytes(), nil
}

//Decode decodes passed in reader into value pointer
func (m *AbstractIntMap) Decode(reader io.Reader, valuePointer interface{}) error {
	err := m.DecoderFactory.Create(reader).Decode(valuePointer)
	if err != nil {
		return err
	}
	return nil
}

//NewAbstractIntMap create a new *NewAbstractIntMap, it takes compactable buffer config, data encoder and decoder.
func NewAbstractIntMap(config *CompatbleBufferConfig, encoderFactory toolbox.EncoderFactory, decoderFactory toolbox.DecoderFactory) (*AbstractIntMap, error) {
	buffer, err := NewCompactableBuffer(config)
	if err != nil {
		return nil, fmt.Errorf("Failed to create foo map - unable to create compatable buffer %v", err)
	}
	return &AbstractIntMap{
		Buffer:         buffer,
		Map:            make(map[int]*EntryAddress),
		Mutex:          &sync.Mutex{},
		EncoderFactory: encoderFactory,
		DecoderFactory: decoderFactory,
	}, nil
}

//AbstractStringMap represent string key based map of int and *EntryAddress
type AbstractStringMap struct {
	Buffer         *CompactableBuffer
	Map            map[string]*EntryAddress
	Mutex          *sync.Mutex
	EncoderFactory toolbox.EncoderFactory
	DecoderFactory toolbox.DecoderFactory
}

//Keys returns a map keys.
func (m *AbstractStringMap) Keys() []string {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	var keys = make([]string, 0)
	for k := range m.Map {
		keys = append(keys, k)
	}
	return keys
}

//Size returns a map size.
func (m *AbstractStringMap) Size() int {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return len(m.Map)
}

//Close closes undelying buffer
func (m *AbstractStringMap) Close() error {
	return m.Buffer.Close()
}

//GetAddress returns an address for the passed in key.
func (m *AbstractStringMap) GetAddress(key string) *EntryAddress {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return m.Map[key]
}

//Encode encodes value into bytes, or returns error
func (m *AbstractStringMap) Encode(value interface{}) ([]byte, error) {
	var data = new(bytes.Buffer)
	err := m.EncoderFactory.Create(data).Encode(value)
	if err != nil {
		return nil, fmt.Errorf("Failed to create encoder %v", err)
	}
	return data.Bytes(), nil
}

//Decode decodes io.Reader into value pointer, or returns error.
func (m *AbstractStringMap) Decode(reader io.Reader, valuePointer interface{}) error {
	err := m.DecoderFactory.Create(reader).Decode(valuePointer)
	if err != nil {
		return err
	}
	return nil
}

//NewAbstractStringMap create a new instance of NewAbstractStringMap. It takes compactable buffer config, data encoder and decoder.
func NewAbstractStringMap(config *CompatbleBufferConfig, encoderFactory toolbox.EncoderFactory, decoderFactory toolbox.DecoderFactory) (*AbstractStringMap, error) {
	buffer, err := NewCompactableBuffer(config)
	if err != nil {
		return nil, fmt.Errorf("Failed to create foo map - unable to create compatable buffer %v", err)
	}
	return &AbstractStringMap{
		Buffer:         buffer,
		Map:            make(map[string]*EntryAddress),
		Mutex:          &sync.Mutex{},
		EncoderFactory: encoderFactory,
		DecoderFactory: decoderFactory,
	}, nil
}
