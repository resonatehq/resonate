FROM golang:1.21 AS builder

WORKDIR /app
COPY . .

RUN go build -o resonate .

FROM golang:1.21

WORKDIR /app
COPY --from=builder /app/resonate .

EXPOSE 8001
EXPOSE 50051

ENTRYPOINT ["./resonate"]
