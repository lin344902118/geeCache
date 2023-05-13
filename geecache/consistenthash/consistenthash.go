package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 哈希函数
type Hash func(data []byte) uint32

// 包含所有hash过的真实节点的map
type Map struct {
	// 哈希函数
	hash Hash
	// 每个真实节点对应的虚拟节点数量
	replicas int
	// 哈希环，包含所有虚拟节点hash值，是有序数组
	keys []int
	// 虚拟节点和真实节点映射表,key是虚拟节点hash值，value是真实节点名称
	hashMap map[int]string
}

// 创建一个Map实例
func New(replicas int, fn Hash) *Map {
	m := &Map{
		hash:     fn,
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// 给map添加真实节点
func (m *Map) Add(realNodes ...string) {
	for _, realNode := range realNodes {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + realNode)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = realNode
		}
	}
	sort.Ints(m.keys)
}

// 通过key获取节点
func (m *Map) Get(key string) string {
	// hash环上没有值
	if len(m.keys) == 0 {
		return ""
	}
	// 获取key的hash值
	hash := int(m.hash([]byte(key)))
	// 顺时针找到key对应的虚拟节点下标
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	// 获取真实节点
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
