package main

import (
	"math/rand"
)

type Generator struct {
	minId          uint64
	maxId          uint64
	checksumLength int

	src  rand.Source
	rand *rand.Rand

	indices []int
	pos     int
}

type Batch struct {
	Ids       []uint64
	Checksums map[uint64][]byte
}

func newGenerator(minId, maxId uint64, checksumLength int, src rand.Source) *Generator {
	return &Generator{
		minId:          minId,
		maxId:          maxId,
		checksumLength: checksumLength,
		src:            src,
		rand:           rand.New(src),
	}
}

func (g *Generator) prepare() {
	g.pos = 0
	g.indices = g.rand.Perm(int(g.maxId - g.minId))
}

func (g *Generator) Done() bool {
	return g.pos >= len(g.indices)
}

func (g *Generator) Next() (id uint64, checksum []byte, ok bool) {
	ok = g.pos < len(g.indices)
	if !ok {
		return
	}

	id = uint64(g.indices[g.pos]) + g.minId
	g.pos++

	checksum = make([]byte, g.checksumLength)
	_, err := g.rand.Read(checksum)
	if err != nil {
		// Rand.Read() always returns nil as err
		panic(err)
	}

	return
}

func (g *Generator) CollectBatch(batchSize int) Batch {
	checksums := make(map[uint64][]byte)
	ids := make([]uint64, batchSize)

	for i := 0; i < batchSize; i++ {
		id, checksum, ok := g.Next()
		if !ok {
			ids = ids[0:i]
			break
		}
		checksums[id] = checksum
		ids[i] = id
	}

	return Batch{
		Ids:       ids,
		Checksums: checksums,
	}
}