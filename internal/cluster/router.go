package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"mini-cassandra/internal/hashring"
	"mini-cassandra/internal/kv"
)

type Router struct {
	localStore        *kv.Store
	nodeID            hashring.NodeID
	selfHost          string
	ring              *hashring.Ring
	httpClient        *http.Client
	replicationFactor int
}

func NewRouter(local *kv.Store, nodeID hashring.NodeID, selfHost string, ring *hashring.Ring, replicationFactor int) *Router {
	if replicationFactor < 1 {
		replicationFactor = 1
	}
	return &Router{
		localStore: local,
		nodeID:     nodeID,
		selfHost:   selfHost,
		ring:       ring,
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
		replicationFactor: replicationFactor,
	}
}

func (r *Router) isLocal(node hashring.NodeInfo) bool {
	return node.ID == r.nodeID
}

type replicaPutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type replicaDeleteRequest struct {
	Key string `json:"key"`
}

// Put: grava em todos os nós de réplica (replicação síncrona simples).
func (r *Router) Put(key, value string) error {
	replicas := r.ring.GetReplicasForKey(key, r.replicationFactor)
	if len(replicas) == 0 {
		return fmt.Errorf("no replicas for key")
	}

	var errs []error

	for _, node := range replicas {
		if r.isLocal(node) {
			r.localStore.Put(key, value)
			continue
		}

		body, _ := json.Marshal(replicaPutRequest{Key: key, Value: value})
		url := fmt.Sprintf("http://%s/internal/replica/put", node.Host)

		resp, err := r.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
		if err != nil {
			errs = append(errs, fmt.Errorf("remote PUT to %s failed: %w", node.Host, err))
			continue
		}
		io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 300 {
			errs = append(errs, fmt.Errorf("remote PUT to %s status=%d", node.Host, resp.StatusCode))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("replication errors: %v", errs)
	}
	return nil
}

// Get: tenta ler dos nós de réplica na ordem.
// Retorna no primeiro nó que responder com sucesso.
func (r *Router) Get(key string) (string, bool, error) {
	replicas := r.ring.GetReplicasForKey(key, r.replicationFactor)
	if len(replicas) == 0 {
		return "", false, fmt.Errorf("no replicas for key")
	}

	for _, node := range replicas {
		if r.isLocal(node) {
			val, ok := r.localStore.Get(key)
			if ok {
				return val, true, nil
			}
			continue
		}

		// GET interno: lê direto do store do nó alvo
		baseURL := fmt.Sprintf("http://%s/internal/replica/get", node.Host)
		reqURL, err := url.Parse(baseURL)
		if err != nil {
			continue
		}
		q := reqURL.Query()
		q.Set("key", key)
		reqURL.RawQuery = q.Encode()
		resp, err := r.httpClient.Get(reqURL.String())
		if err != nil {
			// falha de rede: tenta próximo nó
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			// não tem nesse nó, tenta o próximo
			continue
		}
		if resp.StatusCode >= 300 {
			// erro genérico, tenta próximo
			continue
		}

		return string(body), true, nil
	}

	// se nenhum tiver a chave
	return "", false, nil
}

// Delete: envia DELETE para todos os nós de réplica.
func (r *Router) Delete(key string) error {
	replicas := r.ring.GetReplicasForKey(key, r.replicationFactor)
	if len(replicas) == 0 {
		return fmt.Errorf("no replicas for key")
	}

	var errs []error

	for _, node := range replicas {
		if r.isLocal(node) {
			r.localStore.Delete(key)
			continue
		}

		body, _ := json.Marshal(replicaDeleteRequest{Key: key})
		url := fmt.Sprintf("http://%s/internal/replica/delete", node.Host)

		resp, err := r.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
		if err != nil {
			errs = append(errs, fmt.Errorf("remote DELETE to %s failed: %w", node.Host, err))
			continue
		}
		io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 300 {
			errs = append(errs, fmt.Errorf("remote DELETE to %s status=%d", node.Host, resp.StatusCode))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("delete replication errors: %v", errs)
	}
	return nil
}

// 🔥 Rebalanceia todas as chaves locais com base no ring atual.
// Ideia: para cada key local, checar se este nó ainda é uma réplica;
// se não for, envia para os novos donos e remove localmente.
func (r *Router) RebalanceLocalKeys(ctx context.Context) error {
	log.Printf("[REBALANCE] Starting rebalance for node=%s", r.nodeID)

	keys := r.localStore.Keys()
	moved := 0
	kept := 0

	for _, key := range keys {
		select {
		case <-ctx.Done():
			log.Printf("[REBALANCE] cancelled")
			return ctx.Err()
		default:
		}

		// valor atual
		val, ok := r.localStore.Get(key)
		if !ok {
			continue
		}

		// quem são as réplicas para essa chave no ring novo?
		replicas := r.ring.GetReplicasForKey(key, r.replicationFactor)
		if len(replicas) == 0 {
			// ring vazio? estranho, mas não mexe
			kept++
			continue
		}

		// este nó ainda está na lista de réplicas?
		stillReplica := false
		for _, n := range replicas {
			if r.isLocal(n) {
				stillReplica = true
				break
			}
		}

		if stillReplica {
			kept++
			continue
		}

		// 👉 este nó NÃO deveria mais guardar essa chave
		// então manda pro cluster (Router.Put já grava nos novos donos)
		if err := r.Put(key, val); err != nil {
			log.Printf("[REBALANCE] failed to move key=%s: %v", key, err)
			// por segurança, não apagar local em caso de erro
			continue
		}

		// agora pode remover local
		r.localStore.Delete(key)
		moved++
	}

	log.Printf("[REBALANCE] finished for node=%s: moved=%d kept=%d", r.nodeID, moved, kept)
	return nil
}
