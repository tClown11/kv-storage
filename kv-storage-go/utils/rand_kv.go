package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey 生成测试用的随机 Key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("storage-kv-key-%09d", i))
}

// GetTestValue 生成测试用的随机 value
func GetTestValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte(fmt.Sprintf("storage-kv-value-%s", string(b)))
}
