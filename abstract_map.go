package mmcb

import (
	"bytes"
	"fmt"
	"github.com/viant/toolbox"
	"io"
	"sync"
	"reflect"
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

func (m *AbstractIntMap) Exists(key int) bool {
	_, found := m.Map[key]
	return found
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

func (m *AbstractStringMap) Exists(key string) bool {
	_, found := m.Map[key]
	return found
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

type AbstractMap struct {
	IntMap        *AbstractIntMap
	StringMap     *AbstractStringMap
	IdIntType     bool
	IdColumnName  string
	ValueProvider func() interface{}
}

type IntIdentity interface {
	GetId() int
}

type StringIdentity interface {
	GetId() string
}

func (m *AbstractMap) Put(value interface{}) error {
	if m.IdIntType {

		var data, err = m.IntMap.Encode(value)
		if err != nil {
			return fmt.Errorf("Failed to put data %v", err)
		}
		var key = m.GetIntKey(value)
		address := m.IntMap.GetAddress(key)
		if address == nil {
			address, err = m.IntMap.Buffer.Add(data)
		} else {
			err = m.IntMap.Buffer.Update(address, data)
		}
		if err != nil {
			fmt.Printf("Failed to modify data %v", err)
		}
		m.IntMap.Mutex.Lock()
		defer m.IntMap.Mutex.Unlock()
		m.IntMap.Map[key] = address
	} else {
		var data, err = m.StringMap.Encode(value)
		if err != nil {
			return fmt.Errorf("Failed to put data %v", err)
		}
		var key = m.GetStringKey(value)
		address := m.StringMap.GetAddress(key)
		if address == nil {
			address, err = m.StringMap.Buffer.Add(data)
		} else {
			err = m.StringMap.Buffer.Update(address, data)
		}
		if err != nil {
			fmt.Printf("Failed to modify data %v", err)
		}
		m.StringMap.Mutex.Lock()
		defer m.StringMap.Mutex.Unlock()
		m.StringMap.Map[key] = address
	}

	return nil
}

func (m *AbstractMap) Get(key interface{}) (interface{}, error) {
	if m.IdIntType {
		var intKey = toolbox.AsInt(key);
		address := m.IntMap.GetAddress(intKey)
		if address == nil {
			return nil, nil
		}
		reader, err := m.IntMap.Buffer.EntryReader(address)
		if err != nil {
			return nil, fmt.Errorf("Failed to get data for %v - unable get reader %v", key, err)
		}
		var result interface{}
		err = m.IntMap.DecoderFactory.Create(reader).Decode(&result)
		if err != nil {
			return nil, fmt.Errorf("Failed to get data for %v - unable decode %v", key, err)
		}
		return result, nil
	} else {
		var stringKey = toolbox.AsString(key);
		address := m.StringMap.GetAddress(stringKey)
		if address == nil {
			return nil, nil
		}
		reader, err := m.StringMap.Buffer.EntryReader(address)
		if err != nil {
			return nil, fmt.Errorf("Failed to get data for %v - unable get reader %v", key, err)
		}
		var result interface{}
		err = m.StringMap.DecoderFactory.Create(reader).Decode(&result)
		if err != nil {
			return nil, fmt.Errorf("Failed to get data for %v - unable decode %v", key, err)
		}
		return result, nil
	}
}

func (m *AbstractMap) GetAll() (toolbox.Iterator, error) {
	return newAbstractMapIterator(m), nil
}

func (m *AbstractMap) Exists(key interface{}) bool {
	if m.IdIntType {
		var intKey = toolbox.AsInt(key)
		_, found := m.IntMap.Map[intKey]
		return found
	}

	var stringKey = toolbox.AsString(key)
	_, found := m.StringMap.Map[stringKey]
	return found
}

func (m *AbstractMap) GetInto(key, target interface{}) (err error) {
	value, err := m.Get(key);
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("pkg: %v", r)
			}
		}
	}()
	reflect.ValueOf(target).Elem().Set(reflect.ValueOf(value))
	return err
}

func (m *AbstractMap) Remove(key interface{}) error {
	if m.IdIntType {
		var intKey = toolbox.AsInt(key)
		m.IntMap.Mutex.Lock()
		defer m.IntMap.Mutex.Unlock()
		if address, found := m.IntMap.Map[intKey]; found {
			err := m.IntMap.Buffer.Remove(address)
			if err != nil {
				return fmt.Errorf("Failed to remove key: %v, %v", key, err)
			}
			delete(m.IntMap.Map, intKey)
		}
	} else {
		var stringKey = toolbox.AsString(key)
		m.StringMap.Mutex.Lock()
		defer m.StringMap.Mutex.Unlock()
		if address, found := m.StringMap.Map[stringKey]; found {
			err := m.StringMap.Buffer.Remove(address)
			if err != nil {
				return fmt.Errorf("Failed to remove key: %v, %v", key, err)
			}
			delete(m.StringMap.Map, stringKey)
		}
	}
	return nil
}

func (m *AbstractMap) GetIntKey(value interface{}) int {
	if aMap, ok := value.(*map[string]interface{}); ok {
		 return toolbox.AsInt((*aMap)[m.IdColumnName])
	} else if aMap, ok := value.(map[string]interface{}); ok {
		return toolbox.AsInt((aMap)[m.IdColumnName])
	}
	var provider IntIdentity = value.(IntIdentity)
	return provider.GetId();
}

func (m *AbstractMap) GetStringKey(value interface{}) string {
	if aMap, ok := value.(map[string]interface{}); ok {
		return toolbox.AsString(aMap[m.IdColumnName])
	}
	var provider StringIdentity = value.(StringIdentity)
	return provider.GetId();
}

func (m *AbstractMap) loadPersistedData() error {
	if m.IdIntType {

		addresses, err := m.IntMap.Buffer.Addresses()
		if err != nil {
			return fmt.Errorf("Failed to load persisted data - unable to get addresses %v", err)
		}
		for _, address := range addresses {
			reader, err := m.IntMap.Buffer.EntryReader(address)
			if err != nil {
				return fmt.Errorf("Failed to get  EntryReader %v", err)
			}
			var value = m.ValueProvider()
			err = m.IntMap.Decode(reader, value)
			if err != nil {
				return fmt.Errorf("Failed to load persisted data %v", err)
			}

			var id = m.GetIntKey(value)
			m.IntMap.Map[id] = address
		}

	} else {

		addresses, err := m.StringMap.Buffer.Addresses()
		if err != nil {
			return fmt.Errorf("Failed to load persisted data - unable to get addresses %v", err)
		}
		for _, address := range addresses {
			reader, err := m.StringMap.Buffer.EntryReader(address)
			if err != nil {
				return fmt.Errorf("Failed to get  EntryReader %v", err)
			}
			var value = m.ValueProvider()

			err = m.StringMap.Decode(reader, value)
			if err != nil {
				return fmt.Errorf("Failed to load persisted data %v", err)
			}
			var id = m.GetStringKey(value)
			m.StringMap.Map[id] = address
		}
	}
	return nil
}

func NewAbstractMap(encoderFactory toolbox.EncoderFactory, decoderFactory toolbox.DecoderFactory, config *CompatbleBufferConfig, valueProvider func() interface{}, idIntType bool, idColumnName string) (result *AbstractMap, err error) {
	var abstractIntMap *AbstractIntMap
	var abstractStringMap *AbstractStringMap
	if encoderFactory == nil {
		encoderFactory = toolbox.NewJSONEncoderFactory()
	}
	if decoderFactory == nil {
		decoderFactory = toolbox.NewJSONDecoderFactory()
	}

	if idIntType {
		abstractIntMap, err = NewAbstractIntMap(config, encoderFactory, decoderFactory)
		if err != nil {
			return nil, fmt.Errorf("Failed to create a foo map %v", err)
		}
	} else {
		abstractStringMap, err = NewAbstractStringMap(config, encoderFactory, decoderFactory)
		if err != nil {
			return nil, fmt.Errorf("Failed to create a foo map %v", err)
		}
	}
	result = &AbstractMap{
		IntMap:abstractIntMap,
		StringMap:abstractStringMap,
		IdIntType:idIntType,
		ValueProvider:valueProvider,
		IdColumnName:idColumnName,
	}
	err = result.loadPersistedData()
	if err != nil {
		return nil, fmt.Errorf("Failed to create a foo map %v", err)
	}
	return result, nil
}

func (m *AbstractMap) Close() error {
	if m.IdIntType {
		return m.IntMap.Close()
	}
	return m.StringMap.Close()
}

type abstractMapIterator struct {
	aMap  *AbstractMap
	keys  []interface{}
	index int
	value interface{}
}

func (i *abstractMapIterator) HasNext() bool {
	if i.index >= len(i.keys) {
		return false
	}
	var err error
	for j := i.index; j < len(i.keys); j++ {
		i.value, err = i.aMap.Get(i.keys[j])
		if err == nil {
			return true;
		}
	}
	return false
}


func (i abstractMapIterator) Next(itemPointer interface{}) error {
	mapValue, ok := i.value.(map[string]interface{})
	if ! ok {
		return fmt.Errorf("Value was not a map %T", i.value)
	}
	if mapPointer, ok := itemPointer.(*map[string]interface{}); ok {
		*mapPointer = mapValue
	} else {
		return fmt.Errorf("ItemPointer was not a map %T", itemPointer)
	}
	return nil
}


func newAbstractMapIterator(aMap *AbstractMap) toolbox.Iterator {
	var keys = make([]interface{}, 0)
	if aMap.IdIntType {
		for k, _ := range aMap.IntMap.Map {
			keys = append(keys, k)
		}
	} else {
		for k, _ := range aMap.StringMap.Map {
			keys = append(keys, k)
		}
	}
	return &abstractMapIterator{
		keys:keys,
		aMap:aMap,
	}
}
