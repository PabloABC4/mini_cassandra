# Mini Cassandra

Sistema distribuído de key-value store inspirado no Apache Cassandra, implementado em Go.

## 🚀 Início Rápido

```bash
cd mini_cassandra
docker-compose up --build
```

Isso inicia 3 nós nas portas 8081, 8082 e 8083.

## 📖 Uso

```bash
# Armazenar
curl -X PUT http://localhost:8081/kv/chave -d "valor"

# Recuperar
curl http://localhost:8081/kv/chave

# Deletar
curl -X DELETE http://localhost:8081/kv/chave
```

## 🏗️ Características

- Hash ring com virtual nodes
- Replicação configurável (padrão: 3 réplicas)
- Rebalanceamento automático
- API REST simples

## ⚙️ Configuração

Variáveis de ambiente:
- `NODE_ID`: Identificador do nó
- `LISTEN_ADDR`: Porta de escuta
- `CLUSTER_NODES`: Lista de nós do cluster
- `REPLICATION_FACTOR`: Fator de replicação
