package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tClown11/kv-storage/data"
)

var testBTree = NewBTree(32)

func TestBTree_PUT(t *testing.T) {

	tests := []struct {
		testData *BItem
		result   bool
	}{
		{
			testData: &BItem{pos: &data.LogRecordPos{Fid: 1, Offset: 100}},
			result:   true,
		},
		{
			testData: &BItem{
				key: []byte("a"),
				pos: &data.LogRecordPos{Fid: 1, Offset: 2},
			},
			result: true,
		},
	}

	for i := range tests {
		res := testBTree.Put(tests[i].testData.key, tests[i].testData.pos)
		if tests[i].result != res {
			t.Errorf("put data(%+v) to BTree fail", tests[i].testData)
		}
	}
}

func TestBTree_GET(t *testing.T) {

	tests := []struct {
		testData *BItem
		result   *data.LogRecordPos
	}{
		{
			testData: &BItem{key: []byte("1")},
			result:   nil,
		},
		{
			testData: &BItem{
				key: []byte("a"),
			},
			result: &data.LogRecordPos{Fid: 1, Offset: 2},
		},
	}

	for i := range tests {
		res := testBTree.Get(tests[i].testData.key)
		if !assert.Equal(t, tests[i].result, res) && !(assert.Equal(t, tests[i].result.Fid, res.Fid) && assert.Equal(t, tests[i].result.Offset, res.Offset)) {
			t.Errorf("get data(%+v) from BTree fail", tests[i].testData.key)
		}
	}
}

func TestBTree_DEL(t *testing.T) {
	tests := []struct {
		testData *BItem
		result   bool
	}{
		{
			testData: &BItem{key: []byte("1")},
			result:   false,
		},
		{
			testData: &BItem{
				key: []byte("a"),
			},
			result: true,
		},
	}

	for i := range tests {
		res := testBTree.Delete(tests[i].testData.key)
		if tests[i].result != res {
			t.Errorf("delete data(%+v) from BTree fail", tests[i].testData)
		}
	}
}
