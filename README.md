# Memory Mapped Compactable Buffer

[![GoReportCard](https://goreportcard.com/badge/github.com/viant/mmcb)](https://goreportcard.com/report/github.com/viant/mmcb)
[![GoDoc](https://godoc.org/github.com/viant/mmcb?status.svg)](https://godoc.org/github.com/viant/mmcb)

This library is compatible with Go 1.5+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#Motivation)

- [Usage](#Usage)
- [Examples](#Examples)

- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)



## Motivation

This library was developed to support safe data storage in a memory mapped files, that has ability to self compaction during normal operation.

<a name="Usage"></a>

### Usage

The compactable buffer uses memory mapped file to store addressable entries, it provides based CRUD support with EntryAddress.
'Add' operation allocates extra  memory determined by extensionFactor to leave some space for the future updates.
If there is no space left during updating an entry, data gets migrated to new address and address pointer is updated. 
Deletion operation only flags entry as deleted, to finally be removed from the buffer by compaction process. 
Get operations provides access to DataHeader, DataEntry, EntryReader or EntryReaderAt.
Compaction process can be enabled to run automaticly with either space based or time based policy.


```go

    config, err := mmcb.NewCompatbleBufferConfig(id, filename, 1024*64, 1.2, mmcb.CompactionSpaceBased, 70, 20.0, 1)
	if err != nil {
        ...
	}
	buffer, err := mmcb.NewCompactableBuffer(config)
	address, err := buffer.Add([]byte("abc"))
	if err != nil {
            ...
    }
    err := buffer.Update(address,[]byte("xyz"))
    if err != nil {
        ...
    }
    dataHeader, err := buffer.ReadHeader(address)
    if err != nil {
        ...
    }
    if dataHeader.IsValid() ||  dataHeader.IsRemoved() {
        ...
    }
    dataEntry, err := buffer.ReadEntry(address)
    if err != nil {
            ...
    }
    entryReader, err :=  buffer.EntryReader(address)
    if err != nil {
        ...
    }
    addresses, err := buffer.Addresses()
    if err != nil {
           ...
    }
    err := buffer.Remove(address)
    if err != nil {
        ...
    }
    buffer.Close()
    
```    
    


<a name="Example"></a>

### Example

Memory mapped compactable buffer can be used with a standard map that provides persistency as simple key/value store.

Here is an example how to create memory mapped based int key based map.

Example
```go

	type Foo struct {
		Id   int
		Name string
	}
	
	type IntFooMap struct {
		*mmcb.AbstractIntMap
	}
	
	func (m *IntFooMap) Put(key int, value *Foo) (error) {
		var data, err = m.Encode(value)
		if err != nil {
			return  fmt.Errorf("Failed to put data %v", err)
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
	
	func (m *IntFooMap) Get(key int) (*Foo, error) {
		address := m.GetAddress(key)
		if address == nil {
			return nil, nil
		}
		reader, err := m.Buffer.EntryReader(address)
		if err != nil {
			return nil, fmt.Errorf("Failed to get data for %v - unable get reader %v", key, err)
		}
		var result = &Foo{}
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
			foo := &Foo{}
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
			return nil, fmt.Errorf("Failed to create a foo map %v",err)
		}
		result :=  &IntFooMap{abstractIntMap}
		err = result.loadPersistedData()
		if err != nil {
			return nil, fmt.Errorf("Failed to create a foo map %v",err)
		}
		return result, nil
	}
```

## GoCover

[![GoCover](https://gocover.io/github.com/viant/mmcb)](https://gocover.io/github.com/viant/mmcb)
	
<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:** Adrian Witas

**Contributors:**
