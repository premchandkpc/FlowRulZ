FROM rust:1.85-alpine AS rust-builder
RUN apk add --no-cache gcc musl-dev
WORKDIR /build
COPY runtime/ runtime/
RUN cd runtime && cargo build --release

FROM golang:1.26-alpine AS go-builder
RUN apk add --no-cache gcc musl-dev
WORKDIR /build
COPY . .
COPY --from=rust-builder /build/runtime/target/release/libflowrulz_core.a runtime/target/release/
ENV CGO_ENABLED=1 GOOS=linux
RUN go build -o /flowrulz    ./server/cmd/flowrulz && \
    go build -o /sim         ./simulator/cmd/simulator

FROM alpine:3.21 AS flowrulz
RUN apk add --no-cache ca-certificates libgcc
COPY --from=go-builder /flowrulz /usr/local/bin/flowrulz
EXPOSE 8080 9090
ENTRYPOINT ["flowrulz"]

FROM alpine:3.21 AS sim
RUN apk add --no-cache ca-certificates libgcc
COPY --from=go-builder /sim /usr/local/bin/sim
EXPOSE 8081
ENTRYPOINT ["sim"]
