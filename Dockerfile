FROM golang:1.21 AS builder

WORKDIR /app
COPY . .

ENV CGO_ENABLED 0
RUN go build -o resonate .

FROM scratch

WORKDIR /app
COPY --from=builder /app/resonate .

EXPOSE 8001
EXPOSE 50051

ENTRYPOINT ["./resonate"]
