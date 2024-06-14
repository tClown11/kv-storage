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

func TestBtree_Iterator(t *testing.T) {
	tests := []struct {
		testTree *Btree
		testData []*BItem
		reverse  bool
	}{
		{
			// Btree 为空
			testTree: NewBtree(32),
		},
		{
			// Btree 有数据的情况
			testTree: NewBtree(32),
			testData: []*BItem{
				{[]byte("ccde"), &structure.LogRecordPos{Fid: 1, Offset: 10}},
			},
		},
		{
			// 有多条数据
			testTree: NewBtree(32),
			testData: []*BItem{
				{[]byte("ccde"), &structure.LogRecordPos{Fid: 1, Offset: 10}},
				{[]byte("acee"), &structure.LogRecordPos{Fid: 1, Offset: 20}},
				{[]byte("eede"), &structure.LogRecordPos{Fid: 1, Offset: 30}},
				{[]byte("bbcd"), &structure.LogRecordPos{Fid: 1, Offset: 40}},
			},
			reverse: true,
		},
	}

	for i := range tests {
		for j := range tests[i].testData {
			tests[i].testTree.Put(tests[i].testData[j].key, tests[i].testData[j].pos)
		}
		iter := tests[i].testTree.Iterator(tests[i].reverse)
		for iter.Valid() {
			assert.NotNil(t, iter.Key())
			assert.NotNil(t, iter.Value())
			iter.Next()
		}
		assert.Equal(t, false, iter.Valid())
		for iter.Rewind(); iter.Valid(); iter.Next() {
			assert.NotNil(t, iter.Key())
		}

		for iter.Seek([]byte("cc")); iter.Valid(); iter.Next() {
			assert.NotNil(t, iter.Key())
		}
	}
}
