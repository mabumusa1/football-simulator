FROM golang:1.25-alpine3.23 AS builder
RUN apk add --no-cache git ca-certificates tzdata
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /app/consumer \
    .

# Add runtime
FROM alpine:3.23

RUN apk --no-cache add ca-certificates wget && \
    addgroup -g 1000 appgroup && \
    adduser -u 1000 -G appgroup -s /bin/sh -D gouser

WORKDIR /app
COPY --from=builder /app/consumer .
RUN chown -R gouser:appgroup /app
USER gouser
EXPOSE 9091
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=5 \
    CMD wget -q --spider http://localhost:9091/metrics || exit 1

CMD ["./consumer"]
