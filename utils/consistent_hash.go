package utils

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashFunc func([]byte) uint32

type ConsistentMap struct {
	replicas int
	hash     HashFunc
	keys     []int
	hashMap  map[int]int
}

func NewConsistentHashMap(replicas int, hash HashFunc) *ConsistentMap {
	m := &ConsistentMap{
		replicas: replicas,
		hash:     hash,
		keys:     nil,
		hashMap:  make(map[int]int),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *ConsistentMap) IsEmpty() bool {
	return len(m.keys) == 0
}

func (m *ConsistentMap) Get(str string) int {
	if m.IsEmpty() {
		return -1
	}
	hash := int(m.hash([]byte(str)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}
	return m.hashMap[m.keys[idx]]
}

func (m *ConsistentMap) Add(shards ...int) {
	for _, shard := range shards {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + "shard" + strconv.Itoa(shard))))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = shard
		}
	}
	sort.Ints(m.keys)
}
