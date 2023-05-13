package geecache

import (
	"fmt"
	pb "geeCache/geecache/geecachepb"
	"geeCache/geecache/singleflight"
	"log"
	"sync"
)

// 定义缓存数据来源接口
type Getter interface {
	Get(key string) ([]byte, error)
}

// 定义数据来源函数
type GetterFunc func(key string) ([]byte, error)

// 实现数据来源接口
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// 缓存命名空间，存储不同类型的记录
type Group struct {
	name      string
	getter    Getter
	mainCache cache
	peers     PeerPicker
	loader    *singleflight.Group
}

var (
	mu sync.RWMutex
	// 全局缓存命名空间池
	groups = make(map[string]*Group)
)

// 创建一个Group实例并添加到池中
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
	groups[name] = g
	return g
}

// 从池中获取Group
func GetGroup(name string) *Group {
	mu.RLock()
	defer mu.RUnlock()
	g := groups[name]
	return g
}

// 通过key获取记录
func (g *Group) Get(key string) (ByteView, error) {
	// key为空，返回错误
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	// 从缓存中获取记录
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		return v, nil
	}
	// 缓存没有记录，从数据源加载数据
	return g.load(key)
}

// 注册远端节点
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more then once")
	}
	g.peers = peers
}

// 从数据源加载数据
func (g *Group) load(key string) (value ByteView, err error) {
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err := g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		return g.getLocally(key)
	})
	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

// 从远端节点获取数据
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key: key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: res.Value}, nil
}

// 获取缓存并添加到mainCache中
func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

// 将获取的数据加入缓存
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}
