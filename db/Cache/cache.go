package cache

import "sync"

type CacheActions interface {
	Get(key string) (string, bool)
	Set(key, value string)
	Keys() []string
	Values() []string
}

type Cache struct {
	values map[string]string
	mu     sync.Mutex
	len    int
	maxLen int
}

func NewCache(maxLen int) *Cache {
	return &Cache{values: make(map[string]string), mu: sync.Mutex{}, maxLen: maxLen}
}

func (c *Cache) Get(key string) (string, bool) {
	val, ok := c.values[key]
	return val, ok
}

func (c *Cache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[key] = value
}

func (c *Cache) Keys() []string {
	keys := make([]string, 0, c.len)
	for key := range c.values {
		keys = append(keys, key)
	}

	return keys
}

func (c *Cache) Values() []string {
	vals := make([]string, 0, c.len)
	for _, val := range c.values {
		vals = append(vals, val)
	}

	return vals
}

func (c *Cache) IsAtMaxSize() bool {
	return c.len == c.maxLen
}

func (c *Cache) Swap() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	currCache := c.values
	c.values = make(map[string]string)
	c.len = 0
	return currCache
}
