package mmcb_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/mmcb"
	"testing"
)

func TestUIntConversion(t *testing.T) {

	bytes := make([]byte, 0)
	appended := mmcb.AppendUIntToBytes(uint(5), &bytes)
	assert.Equal(t, 1, appended)

	appended = mmcb.AppendUIntToBytes(uint(512), &bytes)
	assert.Equal(t, 2, appended)

	appended = mmcb.AppendUIntToBytes(uint(32*1024), &bytes)
	assert.Equal(t, 3, appended)

	assert.Equal(t, 6, len(bytes))

	offset := 0
	{
		result, sizeRead, err := mmcb.ReadUIntFromBytes(bytes, uint64(offset))
		assert.Nil(t, err)
		assert.Equal(t, uint(5), result)
		assert.Equal(t, 1, sizeRead)
		offset += sizeRead
	}

	{
		result, sizeRead, err := mmcb.ReadUIntFromBytes(bytes, uint64(offset))
		assert.Nil(t, err)
		assert.Equal(t, uint(512), result)
		assert.Equal(t, 2, sizeRead)
		offset += sizeRead
	}

	{
		result, sizeRead, err := mmcb.ReadUIntFromBytes(bytes, uint64(offset))
		assert.Nil(t, err)
		assert.Equal(t, uint(32*1024), result)
		assert.Equal(t, 3, sizeRead)
	}

}

func TestIntConversion(t *testing.T) {

	bytes := make([]byte, 0)
	appended := mmcb.AppendIntToBytes(int(5), &bytes)
	assert.Equal(t, 1, appended)

	appended = mmcb.AppendIntToBytes(int(512), &bytes)
	assert.Equal(t, 2, appended)

	appended = mmcb.AppendIntToBytes(int(32*1024), &bytes)
	assert.Equal(t, 3, appended)

	assert.Equal(t, 6, len(bytes))

	offset := 0
	{
		result, sizeRead, err := mmcb.ReadIntFromBytes(bytes, uint64(offset))
		assert.Nil(t, err)
		assert.Equal(t, int(5), result)
		assert.Equal(t, 1, sizeRead)
		offset += sizeRead
	}

	{
		result, sizeRead, err := mmcb.ReadIntFromBytes(bytes, uint64(offset))
		assert.Nil(t, err)
		assert.Equal(t, int(512), result)
		assert.Equal(t, 2, sizeRead)
		offset += sizeRead
	}

	{
		result, sizeRead, err := mmcb.ReadIntFromBytes(bytes, uint64(offset))
		assert.Nil(t, err)
		assert.Equal(t, int(32*1024), result)
		assert.Equal(t, 3, sizeRead)
	}

}

func TestByteConversion(t *testing.T) {
	bytes := make([]byte, 0)
	appended := mmcb.AppendToBytes([]byte("abc"), &bytes)
	assert.Equal(t, 4, appended)
	assert.Equal(t, 4, len(bytes))
	appended = mmcb.AppendToBytes([]byte("a"), &bytes)
	assert.Equal(t, 2, appended)
	assert.Equal(t, 6, len(bytes))

	{
		var result []byte
		bytesRead, err := mmcb.ReadBytes(bytes, 0, &result)
		assert.Nil(t, err)
		assert.Equal(t, 4, bytesRead)
		assert.Equal(t, "abc", string(result))

	}
	{
		var result []byte
		bytesRead, err := mmcb.ReadBytes(bytes, 4, &result)
		assert.Nil(t, err)
		assert.Equal(t, 2, bytesRead)
		assert.Equal(t, "a", string(result))
	}

}

func TestReadBytes(t *testing.T) {
	bytes := make([]byte, 1)
	mmcb.AppendToBytes([]byte("abc"), &bytes)
	out := make([]byte, 0)
	_, err := mmcb.ReadBytes(bytes, 1, &out)
	assert.Nil(t, err)
	assert.Equal(t, "abc", string([]byte(out)))
}

func TestAppendUIntToBytes(t *testing.T) {
	bytes := make([]byte, 1)
	mmcb.AppendUIntToBytes(uint(64), &bytes)
	out, _, err := mmcb.ReadUIntFromBytes(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, uint(64), out)
}

func TestVarUIntSize(t *testing.T) {
	assert.Equal(t, 1, mmcb.VarIntSize(1))
	assert.Equal(t, 3, mmcb.VarIntSize(100000))
	assert.Equal(t, 4, mmcb.VarIntSize(100000000))
}

func TestVarIntSize(t *testing.T) {
	assert.Equal(t, 1, mmcb.VarUIntSize(uint(1)))
	assert.Equal(t, 3, mmcb.VarUIntSize(uint(100000)))
	assert.Equal(t, 4, mmcb.VarUIntSize(uint(100000000)))
}
