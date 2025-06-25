# Build. 
FROM cgr.dev/chainguard/go AS builder

WORKDIR /app
COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o resonate . 

# Distribute. 
FROM cgr.dev/chainguard/glibc-dynamic

WORKDIR /app
COPY --from=builder /app/resonate .

EXPOSE 8001
EXPOSE 8002
EXPOSE 50051

ENTRYPOINT ["./resonate"]

