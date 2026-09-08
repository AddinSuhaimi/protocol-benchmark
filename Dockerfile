FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/protocol-harness main.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/protocol-harness /app/
ENTRYPOINT ["/app/protocol-harness"]
