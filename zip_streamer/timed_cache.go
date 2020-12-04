package zip_streamer

import (
	"sync"
	"time"
)

type LinkCache struct {
	cache   sync.Map
	timeout *time.Duration
}

func NewLinkCache(timeout *time.Duration) LinkCache {
	return LinkCache{
		cache:   sync.Map{},
		timeout: timeout,
	}
}

type cacheEntry struct {
	Filename string
	S3Creds  s3Creds
	Entries  []*FileEntry
}

func (c *LinkCache) Get(linkKey string) *cacheEntry {
	result, ok := c.cache.Load(linkKey)
	if ok {
		var entry = result.(cacheEntry)
		return &entry
	} else {
		return nil
	}
}

func (c *LinkCache) Set(linkKey string, entry cacheEntry) {
	c.cache.Store(linkKey, entry)

	if c.timeout != nil {
		go func() {
			time.Sleep(*c.timeout)
			c.cache.Delete(linkKey)
		}()
	}
}
