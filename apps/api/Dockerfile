# Compile the app
FROM golang:1.25-alpine3.23 AS builder
RUN apk add --no-cache ca-certificates git tzdata
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /app/api \
    .

# Add runtime
FROM alpine:3.23

RUN apk --no-cache add ca-certificates wget && \
    addgroup -g 1000 appgroup && \
    adduser -u 1000 -G appgroup -s /bin/sh -D gouser

WORKDIR /app
COPY --from=builder /app/api .
RUN chown -R gouser:appgroup /app
USER gouser
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q --spider http://localhost:8080/health || exit 1

CMD ["./api"]
