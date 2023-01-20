package atur_test

import (
	"math"
	"testing"

	"github.com/lukaproject/atur"
	"github.com/stretchr/testify/require"
)

func Test_Int32(t *testing.T) {
	int32List := []*atur.Int32{
		{V: math.MaxInt32},
		{V: math.MinInt32},
		{V: 0},
		{V: -1111112323},
		{V: 1333333333},
		{V: 0x3333332},
	}
	for _, v := range int32List {
		vByte := v.Serialize()
		vTmp := &atur.Int32{}
		err := vTmp.Unserialize(vByte)
		require.Nilf(t, err, "unserialize failed : %v", err)
		require.Equal(t, v.V, vTmp.V, "not equal")
	}
}

func Test_Int64(t *testing.T) {
	int64List := []*atur.Int64{
		{V: math.MaxInt64},
		{V: math.MinInt64},
		{V: 0},
		{V: -1111112323},
		{V: 1333333333},
		{V: 0x3333332},
	}
	for _, v := range int64List {
		vByte := v.Serialize()
		vTmp := &atur.Int64{}
		err := vTmp.Unserialize(vByte)
		require.Nilf(t, err, "unserialize failed : %v", err)
		require.Equal(t, v.V, vTmp.V, "not equal")
	}
}

func Test_Uint32(t *testing.T) {
	uint32List := []*atur.Uint32{
		{V: math.MaxUint32},
		{V: 0},
		{V: 1111112323},
		{V: 1333333333},
		{V: 0x3333332},
	}
	for _, v := range uint32List {
		vByte := v.Serialize()
		vTmp := &atur.Uint32{}
		err := vTmp.Unserialize(vByte)
		require.Nilf(t, err, "unserialize failed : %v", err)
		require.Equal(t, v.V, vTmp.V, "not equal")
	}
}

func Test_Uint64(t *testing.T) {
	uint64List := []*atur.Uint64{
		{V: math.MaxInt64},
		{V: 0},
		{V: 1111112323},
		{V: 1333333333},
		{V: 0x3333332},
	}
	for _, v := range uint64List {
		vByte := v.Serialize()
		vTmp := &atur.Uint64{}
		err := vTmp.Unserialize(vByte)
		require.Nilf(t, err, "unserialize failed : %v", err)
		require.Equal(t, v.V, vTmp.V, "not equal")
	}
}
