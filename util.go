package atur

import (
	"context"

	"github.com/spaolacci/murmur3"
)

func calcIdx(key []byte, shard int) uint32 {
	m3 := murmur3.New32()
	m3.Write(key)
	return m3.Sum32() % uint32(shard)
}

func checkCtxTimeout(ctx context.Context) error {
	select {
	default:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func InterfaceToWithError[T any](i interface{}, err error) (T, error) {
	return i.(T), err
}

func InterfaceToWithBool[T any](i interface{}, b bool) (T, bool) {
	return i.(T), b
}

func InterfaceTo[T any](i interface{}) T {
	return i.(T)
}
