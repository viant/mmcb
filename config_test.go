package mmcb_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/mmcb"
	"testing"
)

func TestValidate(t *testing.T) {

	_, err := mmcb.NewCompatbleBufferConfig(uint8(1), "/tmp/compatableBuffer.mmf", 1024*32, 1.2, 10, 0.0, 0.0, 0)
	assert.NotNil(t, err, "Should not accept invalid policy")

	{ //frequency policy test
		_, err = mmcb.NewCompatbleBufferConfig(uint8(1), "/tmp/compatableBuffer.mmf", 1024*32, 1.2, mmcb.CompactionFrequencyBased, 0.0, 0.0, 0)
		assert.NotNil(t, err, "Should have frequency set")

		_, err = mmcb.NewCompatbleBufferConfig(uint8(1), "/tmp/compatableBuffer.mmf", 1024*32, 1.2, mmcb.CompactionFrequencyBased, 0.0, 0.0, 1)
		assert.Nil(t, err, "Should have frequency set")

	}

	{ //frequency policy test
		_, err = mmcb.NewCompatbleBufferConfig(uint8(1), "/tmp/compatableBuffer.mmf", 1024*32, 1.2, mmcb.CompactionSpaceBased, 0.0, 0.0, 0)
		assert.NotNil(t, err, "Should have frequency set")

		_, err = mmcb.NewCompatbleBufferConfig(uint8(1), "/tmp/compatableBuffer.mmf", 1024*32, 1.2, mmcb.CompactionSpaceBased, 0.0, 0.0, 1)
		assert.NotNil(t, err, "Should have frequency set")

		_, err = mmcb.NewCompatbleBufferConfig(uint8(1), "/tmp/compatableBuffer.mmf", 1024*32, 1.2, mmcb.CompactionSpaceBased, 0.0, 1.0, 1)
		assert.NotNil(t, err, "Should have MinFreeSpacetPct set")

		_, err = mmcb.NewCompatbleBufferConfig(uint8(1), "/tmp/compatableBuffer.mmf", 1024*32, 1.2, mmcb.CompactionSpaceBased, 1.0, 1.0, 1)
		assert.Nil(t, err, "Should have DeletionPctThreshold set")

	}

}
