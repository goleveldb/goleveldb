package block

import (
	"fmt"
	"github.com/goleveldb/goleveldb/common"
	"testing"

	"github.com/goleveldb/goleveldb/slice"
)

type entry struct {
	key   slice.Slice
	value slice.Slice
}

type testCase struct {
	name         string
	writeEntries []*entry
}

func Test_Iter(t *testing.T) {
	testCases := []*testCase{
		{
			name: "short key, total count < 16",
			writeEntries: []*entry{
				makeEntry("20185081", "Li, Junyu"),
				makeEntry("20189999", "qiezi wdnmd"),
				makeEntry("&*%^", "wdnmd"),
				makeEntry("hello", "world"),
				makeEntry("world", "hello"),
			},
		},
		{
			name:         "prefix compression & total count > 16",
			writeEntries: entriesWithFixValue("wdnmd", "wdnmd_%d", 888),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			blockWriter := NewWriter()
			sortEntries(testCase.writeEntries)
			for _, entry := range testCase.writeEntries {
				if err := blockWriter.AddEntry(entry.key, entry.value); err != nil {
					t.Fatal(err)
				}
			}

			iter := NewIter(New(blockWriter.Finish()))
			for i, length := 0, len(testCase.writeEntries); i < length; i++ {
				doTest(t, i, testCase.writeEntries, iter)
			}
		})
	}
}

func entriesWithFixValue(fixedValue string, prefix string, genCount int) []*entry {
	res := make([]*entry, genCount)
	for i := 0; i < genCount; i++ {
		res[i] = makeEntry(fmt.Sprintf(prefix, i), fixedValue)
	}

	sortEntries(res)
	return res
}

func doTest(t *testing.T, i int, entries []*entry, iter common.Iterator) {
	curEntry := entries[i]
	iter.Find(curEntry.key)
	assertTrue(t, iter.Success(), fmt.Sprintf("key %s not found", curEntry.key))

	testPrev(t, iter, entries, i)
	iter.Find(curEntry.key)
	testNext(t, iter, entries, i)
}

func testPrev(t *testing.T, iter common.Iterator, entries []*entry, index int) {
	for index > 0 {
		index--
		iter.Prev()
		assertTrue(t, iter.Success(), "iter returns false while it should be true")
		assertTrue(t, iter.Key().Compare(entries[index].key) == 0,
			fmt.Sprintf("entries[%d].key:%s, iter.key:%s", index, entries[index].key, iter.Key()))
		assertTrue(t, iter.Value().Compare(entries[index].value) == 0,
			fmt.Sprintf("entries[%d].value:%s, iter.value:%s", index, entries[index].value, iter.Value()))
	}

	iter.Prev()
	assertFalse(t, iter.Success(), "iter.Success() should return false")
}

func testNext(t *testing.T, iter common.Iterator, entries []*entry, index int) {
	for index < len(entries)-1 {
		index++
		iter.Next()
		assertTrue(t, iter.Success(), "iter returns false on iter.Next()")
		assertTrue(t, iter.Key().Compare(entries[index].key) == 0,
			fmt.Sprintf("iter.key %s != entries[%d].key %s", iter.Key(), index, entries[index].key))
		assertTrue(t, iter.Value().Compare(entries[index].value) == 0,
			fmt.Sprintf("iter.value %s != entries[%d].value %s", iter.Value(), index, entries[index].value))
	}

	iter.Next()
	assertFalse(t, iter.Success(), "iter.Success() should return false")
}

func assertTrue(t *testing.T, boolVal bool, assertMsg string) {
	if !boolVal {
		t.Fatal(assertMsg)
	}
}

func assertFalse(t *testing.T, boolVal bool, assertMsg string) {
	assertTrue(t, !boolVal, assertMsg)
}

func sortEntries(entries []*entry) {
	for i, length := 0, len(entries); i < length-1; i++ {
		for j := 0; j < length-1-i; j++ {
			if entries[j].key.Compare(entries[j+1].key) <= 0 {
				continue
			}

			entries[j], entries[j+1] = entries[j+1], entries[j]
		}
	}
}

func makeEntry(key, value string) *entry {
	return &entry{
		key:   []byte(key),
		value: []byte(value),
	}
}
