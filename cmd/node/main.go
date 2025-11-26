package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"mini-cassandra/internal/api"
	"mini-cassandra/internal/cluster"
	"mini-cassandra/internal/hashring"
	"mini-cassandra/internal/kv"
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// CLUSTER_NODES: "node1=localhost:8081,node2=localhost:8082,node3=localhost:8083"
func parseClusterNodes(env string) []hashring.NodeInfo {
	if env == "" {
		return nil
	}
	parts := strings.Split(env, ",")
	nodes := make([]hashring.NodeInfo, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		pair := strings.SplitN(p, "=", 2)
		if len(pair) != 2 {
			log.Printf("[WARN] invalid CLUSTER_NODES entry: %s", p)
			continue
		}
		id := pair[0]
		host := pair[1]
		nodes = append(nodes, hashring.NodeInfo{
			ID:   hashring.NodeID(id),
			Host: host,
		})
	}
	return nodes
}

func findSelfHost(nodes []hashring.NodeInfo, nodeID string, listenAddr string) string {
	for _, n := range nodes {
		if string(n.ID) == nodeID {
			return n.Host
		}
	}
	// fallback: monta algo a partir do listenAddr
	// Ex: LISTEN_ADDR=":8080" -> "localhost:8080"
	addr := strings.TrimPrefix(listenAddr, ":")
	if addr == "" {
		addr = "8080"
	}
	return "localhost:" + addr
}

func main() {
	nodeID := getEnv("NODE_ID", "node1")
	listenAddr := getEnv("LISTEN_ADDR", ":8081")
	clusterEnv := getEnv("CLUSTER_NODES", "")
	vNodes := 100
	repFactor := getEnvInt("REPLICATION_FACTOR", 3)

	log.Printf("[BOOT] Starting node %s on %s", nodeID, listenAddr)

	store := kv.NewStore()

	nodes := parseClusterNodes(clusterEnv)
	if len(nodes) == 0 {
		log.Printf("[RING] No CLUSTER_NODES set, using single-node ring")
		selfHost := findSelfHost(nil, nodeID, listenAddr)
		nodes = []hashring.NodeInfo{
			{ID: hashring.NodeID(nodeID), Host: selfHost},
		}
	} else {
		log.Printf("[RING] Loaded %d nodes from CLUSTER_NODES", len(nodes))
	}

	ring := hashring.NewRing(nodes, vNodes)
	selfHost := findSelfHost(nodes, nodeID, listenAddr)

	log.Printf("[NODE] Self host resolved as %s", selfHost)
	log.Printf("[REPL] Replication factor = %d", repFactor)

	router := cluster.NewRouter(store, hashring.NodeID(nodeID), selfHost, ring, repFactor)

	// 🔥 iniciar rebalance em background
	go func() {
		// pequeno delay pra todo mundo subir (ajuste se quiser)
		time.Sleep(5 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := router.RebalanceLocalKeys(ctx); err != nil {
			log.Printf("[REBALANCE] error: %v", err)
		}
	}()

	r := mux.NewRouter()

	// externos (cliente)
	r.HandleFunc("/kv/{key}", api.HandlePutDistributed(router)).Methods("PUT")
	r.HandleFunc("/kv/{key}", api.HandleGetDistributed(router)).Methods("GET")
	r.HandleFunc("/kv/{key}", api.HandleDeleteDistributed(router)).Methods("DELETE")

	// internos (replicação)
	r.HandleFunc("/internal/replica/put", api.HandleReplicaPut(store)).Methods("POST")
	r.HandleFunc("/internal/replica/get", api.HandleReplicaGet(store)).Methods("GET")
	r.HandleFunc("/internal/replica/delete", api.HandleReplicaDelete(store)).Methods("POST")

	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK")
	})

	r.HandleFunc("/debug/keys", api.HandleDebugKeys(store)).Methods("GET")

	log.Printf("[HTTP] Listening on %s", listenAddr)
	if err := http.ListenAndServe(listenAddr, r); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
