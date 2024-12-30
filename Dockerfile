FROM golang:1.23 AS builder

WORKDIR /app
COPY . .

RUN make staticbuild

FROM alpine
COPY --from=builder /app/pktloader /app/pktloader

ENTRYPOINT ["/app/pktloader"]
