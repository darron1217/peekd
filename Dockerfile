FROM golang:1.24.1-alpine AS builder

WORKDIR /app

RUN apk add --no-cache build-base
ENV CGO_ENABLED=1

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN cd ./cmd && go build -o /app/peekd

FROM alpine:3.21

WORKDIR /app

COPY --from=builder /app/peekd /app/

ENTRYPOINT ["/app/peekd"]