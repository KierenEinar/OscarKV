package utils

import "sync"

type sizedBytePool struct {
	sizes []int
	pools []sync.Pool
}

func newSizedBytePool(sizes []int) *sizedBytePool {
	p := &sizedBytePool{
		sizes: sizes,
		pools: make([]sync.Pool, len(sizes)),
	}
	for i := range sizes {
		size := sizes[i]
		p.pools[i].New = func() any {
			return make([]byte, size)
		}
	}
	return p
}

func (p *sizedBytePool) get(n int) []byte {
	if n <= 0 {
		return nil
	}
	for i, size := range p.sizes {
		if n <= size {
			b := p.pools[i].Get().([]byte)
			return b[:n]
		}
	}
	return make([]byte, n)
}

func (p *sizedBytePool) put(b []byte) {
	if b == nil {
		return
	}
	c := cap(b)
	if c == 0 {
		return
	}
	for i, size := range p.sizes {
		if c == size {
			p.pools[i].Put(b[:c])
			return
		}
	}
}

var bytePool = newSizedBytePool([]int{
	poolSize32B,
	poolSize64B,
	poolSize128B,
	poolSize256B,
	poolSize512B,
	poolSize1KiB,
	poolSize2KiB,
	poolSize4KiB,
	poolSize8KiB,
	poolSize16KiB,
	poolSize32KiB,
	poolSize64KiB,
	poolSize128KiB,
	poolSize256KiB,
	poolSize512KiB,
	poolSize1MiB,
})

const (
	poolSize32B = 1 << (5 + iota)  // 32B
	poolSize64B                    // 64B
	poolSize128B                   // 128B
	poolSize256B                   // 256B
	poolSize512B                   // 512B
	poolSize1KiB                   // 1KiB (1024B)
	poolSize2KiB                   // 2KiB (2048B)
	poolSize4KiB                   // 4KiB (4096B)
	poolSize8KiB                   // 8KiB (8192B)
	poolSize16KiB                  // 16KiB (16384B)
	poolSize32KiB                  // 32KiB (32768B)
	poolSize64KiB                  // 64KiB (65536B)
	poolSize128KiB                 // 128KiB (131072B)
	poolSize256KiB                 // 256KiB (262144B)
	poolSize512KiB                 // 512KiB (524288B)
	poolSize1MiB                   // 1MiB (1048576B)
)

func GetBytes(n int) []byte {
	return bytePool.get(n)
}

func PutBytes(b []byte) {
	bytePool.put(b)
}
