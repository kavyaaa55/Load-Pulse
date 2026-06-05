# ── Stage 1: Build ───────────────────────────────────────────────────────────
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Cache dependency downloads separately from source compilation.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o live-sale .

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM alpine:3.18

WORKDIR /app

# ca-certificates is needed for TLS connections (e.g. to managed Redis/Postgres).
RUN apk --no-cache add ca-certificates

COPY --from=builder /app/live-sale .
COPY --from=builder /app/static ./static

EXPOSE 8080

ENTRYPOINT ["./live-sale"]
