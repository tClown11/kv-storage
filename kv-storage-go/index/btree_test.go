package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tClown11/kv-storage/structure"
)

var testBtree = NewBtree(32)

func TestBtree_PUT(t *testing.T) {

	tests := []struct {
		testData *BItem
		result   *structure.LogRecordPos
	}{
		{
			testData: &BItem{pos: &structure.LogRecordPos{Fid: 1, Offset: 100}},
			result:   nil,
		},
		{
			testData: &BItem{
				key: []byte("a"),
				pos: &structure.LogRecordPos{Fid: 1, Offset: 2},
			},
			result: nil,
		},
		{
			testData: &BItem{
				key: []byte("a"),
				pos: &structure.LogRecordPos{Fid: 11, Offset: 12},
			},
			result: &structure.LogRecordPos{Fid: 1, Offset: 2},
		},
	}

	for i := range tests {
		res := testBtree.Put(tests[i].testData.key, tests[i].testData.pos)
		if res != nil &&
			tests[i].result.Fid != res.Fid &&
			tests[i].result.Offset != res.Offset &&
			tests[i].result.Size != res.Size {
			t.Errorf("put data(%+v) to Btree fail", tests[i].testData)
		}
	}
}

func TestBtree_GET(t *testing.T) {

	tests := []struct {
		testData *BItem
		result   *structure.LogRecordPos
	}{
		{
			testData: &BItem{key: []byte("1")},
			result:   nil,
		},
		{
			testData: &BItem{
				key: []byte("a"),
			},
			result: &structure.LogRecordPos{Fid: 11, Offset: 12},
		},
	}

	for i := range tests {
		res := testBtree.Get(tests[i].testData.key)
		if !assert.Equal(t, tests[i].result, res) && !(assert.Equal(t, tests[i].result.Fid, res.Fid) && assert.Equal(t, tests[i].result.Offset, res.Offset)) {
			t.Errorf("get data(%+v) from Btree fail", tests[i].testData.key)
		}
	}
}

func TestBtree_DEL(t *testing.T) {
	tests := []struct {
		testData *BItem
		result   *structure.LogRecordPos
		output   bool
	}{
		{
			testData: &BItem{key: []byte("1")},
			output:   false,
		},
		{
			testData: &BItem{
				key: []byte("a"),
			},
			result: &structure.LogRecordPos{Fid: 1, Offset: 2},
			output: true,
		},
	}

	for i := range tests {
		res, output := testBtree.Delete(tests[i].testData.key)
		if tests[i].result != res && tests[i].output != output {
			t.Errorf("delete data(%+v) from Btree fail", tests[i].testData)
		}
	}
}
