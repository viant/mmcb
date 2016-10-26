package mmcb

import (
	"errors"
	"fmt"
)

const (
	//CompactionDisabled disables auto compaction.
	CompactionDisabled = iota
	//CompactionSpaceBased enables space based compaction.
	CompactionSpaceBased
	//CompactionFrequencyBased enables frequency based compaction
	CompactionFrequencyBased
)

//BufferConfig represents a addressable buffer config.
type BufferConfig struct {
	Filename        string
	ExtensionFactor float32 //control additional space allocation on add operation
	BufferId        uint8   //buffer id
	InitialSize     int     //initial buffer size
}

//CompatbleBufferConfig represents a compactable buffer config.
type CompatbleBufferConfig struct {
	*BufferConfig
	AutoCompactionPolicy  int
	DeletionPctThreshold  float32
	RequiredFreeSpacetPct float32
	FrequencyInSec        int
}

//Validate valides that config is valid or returns an error.
func (c *CompatbleBufferConfig) Validate() error {
	var errorMessage string
	switch c.AutoCompactionPolicy {
	case CompactionDisabled:
		break
	case CompactionSpaceBased:
		if c.DeletionPctThreshold == 0 {
			errorMessage += " CompatbleBufferConfig.DeletionPctThreshold was not set, "
		}
		if c.RequiredFreeSpacetPct == 0 {
			errorMessage += " CompatbleBufferConfig.RequiredFreeSpacetPct was not set, "
		}
		if c.FrequencyInSec == 0 {
			errorMessage += " CompatbleBufferConfig.FrequencyInSec was not set, "
		}
	case CompactionFrequencyBased:
		if c.FrequencyInSec == 0 {
			errorMessage += " CompatbleBufferConfig.FrequencyInSec was not set, "
		}
	default:
		return fmt.Errorf("Unknown compaction policy: %v", c.AutoCompactionPolicy)
	}

	if len(errorMessage) > 0 {
		return errors.New(errorMessage)
	}
	return nil
}

//NewCompatbleBufferConfig creates new compactable buffer. It takes buffer id, filename, initial size, extension factor and compaction policy details.
func NewCompatbleBufferConfig(bufferId uint8, filename string, initialSize int, extensionFactor float32, compactionPolicy int, deletionPctThreshold, minFreeSpacetPct float32, frequencyInSec int) (*CompatbleBufferConfig, error) {
	bufferConfig := &BufferConfig{
		BufferId:        bufferId,
		Filename:        filename,
		InitialSize:     initialSize,
		ExtensionFactor: extensionFactor,
	}
	result := &CompatbleBufferConfig{
		BufferConfig:          bufferConfig,
		AutoCompactionPolicy:  compactionPolicy,
		DeletionPctThreshold:  deletionPctThreshold,
		RequiredFreeSpacetPct: minFreeSpacetPct,
		FrequencyInSec:        frequencyInSec,
	}
	err := result.Validate()
	if err != nil {
		return nil, err
	}
	return result, nil
}
