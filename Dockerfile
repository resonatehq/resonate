# Build. 
FROM cgr.dev/chainguard/go AS builder

ARG COMMIT_HASH

WORKDIR /app
COPY . .

RUN CGO_ENABLED=1 \
    GOOS=linux \
    go \
    build \
    -ldflags="-X github.com/resonatehq/resonate/internal/version.commit=${COMMIT_HASH}" \
    -o resonate .

# Distribute. 
FROM cgr.dev/chainguard/glibc-dynamic

WORKDIR /app
COPY --from=builder /app/resonate .

EXPOSE 8001
EXPOSE 50051

ENTRYPOINT ["./resonate"]

