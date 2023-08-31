package util

import (
	"cmp"
	"sort"
)

func Assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

type KV[K any, V any] struct {
	Key   K
	Value V
}

func OrderedRange[K cmp.Ordered, V any](m map[K]V) []V {
	sorted := make([]V, len(m))
	for i, key := range orderedRangeSort(m) {
		sorted[i] = m[key]
	}

	return sorted
}

func OrderedRangeKV[K cmp.Ordered, V any](m map[K]V) []*KV[K, V] {
	sorted := make([]*KV[K, V], len(m))
	for i, key := range orderedRangeSort(m) {
		sorted[i] = &KV[K, V]{
			Key:   key,
			Value: m[key],
		}
	}

	return sorted
}

func orderedRangeSort[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, len(m))

	i := 0
	for key := range m { // nosemgrep: range-over-map
		keys[i] = key
		i++
	}

	sort.SliceStable(keys, cmp.Less)
	return keys
}
