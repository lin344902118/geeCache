package lru

import "container/list"

// LRU缓存，不支持并发访问
type Cache struct {
	// 可存放最大长度
	maxBytes int64
	// 已经存放的长度
	nbytes int64
	// 双向链表
	ll *list.List
	// 缓存
	cache map[string]*list.Element
	// 可选 当删除时触发的回调函数
	OnEvicted func(key string, value Value)
}

type entry struct {
	key   string
	value Value
}

type Value interface {
	Len() int
}

// 创建新缓存
func New(maxBytes int64, onEvicted func(key string, value Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

func (c *Cache) Get(key string) (value Value, ok bool) {
	// 从缓存中查找记录
	ele, ok := c.cache[key]
	// 找到记录
	if ok {
		// 将记录移动到队尾，并返回记录
		c.ll.MoveToFront(ele)
		e := ele.Value.(*entry)
		return e.value, true
	}
	// 未找到记录，返回空
	return nil, false
}

func (c *Cache) RemoveOldest() {
	// 从双向链表中查找队首记录
	ele := c.ll.Back()
	// 记录不为空
	if ele != nil {
		// 从双向链表和缓存中删除
		c.ll.Remove(ele)
		e := ele.Value.(*entry)
		delete(c.cache, e.key)
		// 更新缓存中使用的记录长度
		c.nbytes -= int64(len(e.key)) + int64(e.value.Len())
		// 如果定义了回调函数则执行
		if c.OnEvicted != nil {
			c.OnEvicted(e.key, e.value)
		}
	}
}

func (c *Cache) Add(key string, value Value) {
	// 判断记录是否已经存在
	ele, ok := c.cache[key]
	// 记录存在，更新数据并更新长度
	if ok {
		// 将记录移动到队尾
		c.ll.MoveToFront(ele)
		// 更新数据
		e := ele.Value.(*entry)
		// 更新Value长度差
		c.nbytes -= int64(e.value.Len()) - int64(value.Len())
		e.value = value
	} else {
		// 记录不存在,将记录加入双向链表队尾并加入缓存
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
		// 更新长度
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	// 如果缓存长度不够用，删除最近最少使用的记录
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

func (c *Cache) Len() int {
	return c.ll.Len()
}
