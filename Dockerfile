FROM golang:1.26-alpine AS go-builder
RUN apk add --no-cache gcc musl-dev
COPY . .
RUN mkdir -p /go/rust/target/release && \
    cp flowrulz-lib/libflowrulz_core.a /go/rust/target/release/
ENV CGO_ENABLED=1
ENV GOOS=linux
RUN go build -mod=vendor -o /flowrulz ./go/cmd/flowrulz

FROM alpine:3.21
RUN apk add --no-cache ca-certificates libgcc
COPY --from=go-builder /flowrulz /usr/local/bin/flowrulz
EXPOSE 8080
ENTRYPOINT ["flowrulz"]
