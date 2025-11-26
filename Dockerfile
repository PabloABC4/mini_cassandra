# Etapa 1: build do binário
FROM golang:1.22 AS builder

WORKDIR /app

# Copia go.mod / go.sum e baixa deps
COPY go.mod go.sum ./
RUN go mod download

# Copia código
COPY . .

# Compila o binário do node
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o node ./cmd/node

# Etapa 2: imagem final
FROM alpine:3.20

WORKDIR /app

# Copia o binário
COPY --from=builder /app/node /app/node

# Porta padrão dentro do container
EXPOSE 8080

# Variáveis padrão (podem ser sobrescritas pelo docker-compose)
ENV NODE_ID=node1
ENV LISTEN_ADDR=:8080
ENV CLUSTER_NODES=node1=node1:8080
ENV REPLICATION_FACTOR=3

CMD ["/app/node"]
