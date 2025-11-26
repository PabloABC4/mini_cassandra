package hashring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

type NodeID string

type NodeInfo struct {
	ID   NodeID
	Host string // host:port
}

type Ring struct {
	mu      sync.RWMutex
	vNodes  int
	hashes  []uint32            // posições ordenadas no anel
	hashMap map[uint32]NodeInfo // hash -> nó "físico"
}

// NewRing cria um ring com N virtual nodes por nó.
func NewRing(nodes []NodeInfo, vNodes int) *Ring {
	r := &Ring{
		vNodes:  vNodes,
		hashMap: make(map[uint32]NodeInfo),
	}
	for _, n := range nodes {
		r.addNodeNoLock(n)
	}
	r.sortHashes()
	return r
}

func (r *Ring) sortHashes() {
	sort.Slice(r.hashes, func(i, j int) bool {
		return r.hashes[i] < r.hashes[j]
	})
}

// hashFn gera um hash 32 bits para uma string.
func hashFn(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// AddNode adiciona um nó ao ring.
func (r *Ring) AddNode(n NodeInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addNodeNoLock(n)
	r.sortHashes()
}

func (r *Ring) addNodeNoLock(n NodeInfo) {
	for i := 0; i < r.vNodes; i++ {
		vKey := fmt.Sprintf("%s#%d", string(n.ID), i)
		h := hashFn(vKey)
		r.hashes = append(r.hashes, h)
		r.hashMap[h] = n
	}
}

// RemoveNode remove um nó (por ID) do ring.
func (r *Ring) RemoveNode(id NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	newHashes := make([]uint32, 0, len(r.hashes))
	for _, h := range r.hashes {
		if r.hashMap[h].ID == id {
			delete(r.hashMap, h)
			continue
		}
		newHashes = append(newHashes, h)
	}
	r.hashes = newHashes
	r.sortHashes()
}

// getNode retorna o nó responsável por um hash de chave (deve ser chamado com lock).
func (r *Ring) getNode(hash uint32) (NodeInfo, bool) {
	if len(r.hashes) == 0 {
		return NodeInfo{}, false
	}
	// busca binária do primeiro hash >= keyHash
	idx := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= hash
	})
	if idx == len(r.hashes) {
		idx = 0 // wrap-around
	}
	h := r.hashes[idx]
	n, ok := r.hashMap[h]
	return n, ok
}

// GetNodeForKey retorna o nó primário para uma chave.
func (r *Ring) GetNodeForKey(key string) (NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h := hashFn(key)
	return r.getNode(h)
}

// GetReplicasForKey retorna até rFactor nós distintos para a chave.
func (r *Ring) GetReplicasForKey(key string, rFactor int) []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 || rFactor <= 0 {
		return nil
	}
	if rFactor > len(r.hashes) {
		rFactor = len(r.hashes)
	}

	h := hashFn(key)
	replicas := make([]NodeInfo, 0, rFactor)
	seen := make(map[NodeID]struct{})

	// posição inicial
	idx := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= h
	})
	if idx == len(r.hashes) {
		idx = 0
	}

	for len(replicas) < rFactor {
		hash := r.hashes[idx]
		node := r.hashMap[hash]
		if _, ok := seen[node.ID]; !ok {
			seen[node.ID] = struct{}{}
			replicas = append(replicas, node)
		}
		idx = (idx + 1) % len(r.hashes)
		// para segurança: se rodar o anel todo, para
		if len(seen) == len(r.hashMap) {
			break
		}
	}

	return replicas
}
