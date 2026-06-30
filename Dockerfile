FROM rust:1.85-alpine AS rust-builder
RUN apk add --no-cache gcc musl-dev
WORKDIR /build
COPY rust/ rust/
RUN cd rust && cargo build --release

FROM golang:1.26-alpine AS go-builder
RUN apk add --no-cache gcc musl-dev
COPY . .
COPY --from=rust-builder /build/rust/target/release/libflowrulz_core.a /go/rust/target/release/
ENV CGO_ENABLED=1
ENV GOOS=linux
RUN go build -mod=vendor -o /flowrulz ./go/cmd/flowrulz

FROM alpine:3.21
RUN apk add --no-cache ca-certificates libgcc
COPY --from=go-builder /flowrulz /usr/local/bin/flowrulz
EXPOSE 8080
ENTRYPOINT ["flowrulz"]
