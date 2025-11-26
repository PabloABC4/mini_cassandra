package api

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"mini-cassandra/internal/cluster"
	"mini-cassandra/internal/kv"

	"github.com/gorilla/mux"
)

func HandlePutDistributed(r *cluster.Router) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		key := mux.Vars(req)["key"]
		body, _ := io.ReadAll(req.Body)
		value := string(body)

		log.Printf("[API] PUT key=%s", key)

		if err := r.Put(key, value); err != nil {
			log.Printf("[ERROR] PUT key=%s err=%v", key, err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func HandleGetDistributed(r *cluster.Router) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		key := mux.Vars(req)["key"]

		log.Printf("[API] GET key=%s", key)

		value, ok, err := r.Get(key)
		if err != nil {
			log.Printf("[ERROR] GET key=%s err=%v", key, err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(value))
	}
}

func HandleDeleteDistributed(r *cluster.Router) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		key := mux.Vars(req)["key"]

		log.Printf("[API] DELETE key=%s", key)

		if err := r.Delete(key); err != nil {
			log.Printf("[ERROR] DELETE key=%s err=%v", key, err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func HandleDebugKeys(store *kv.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		keys := store.Keys()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(keys)
	}
}

type replicaPutReq struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type replicaDeleteReq struct {
	Key string `json:"key"`
}

func HandleReplicaPut(store *kv.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req replicaPutReq
		body, _ := io.ReadAll(r.Body)

		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		// 🔥 Log importantíssimo
		log.Printf("[REPLICA] PUT key=%s value=%s", req.Key, req.Value)

		store.Put(req.Key, req.Value)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func HandleReplicaGet(store *kv.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		// 🔥 Log do GET interno
		log.Printf("[REPLICA] GET key=%s", key)

		val, ok := store.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(val))
	}
}

func HandleReplicaDelete(store *kv.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req replicaDeleteReq
		body, _ := io.ReadAll(r.Body)

		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		// 🔥 Log do DELETE interno
		log.Printf("[REPLICA] DELETE key=%s", req.Key)

		store.Delete(req.Key)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func NotImplemented(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
