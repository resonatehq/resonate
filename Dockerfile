FROM cgr.dev/chainguard/go AS builder

WORKDIR /app
COPY . .

RUN go build -o resonate .

FROM cgr.dev/chainguard/glibc-dynamic

WORKDIR /app
COPY --from=builder /app/resonate .

EXPOSE 8001
EXPOSE 50051

ENTRYPOINT ["./resonate"]
