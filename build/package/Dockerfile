FROM golang:1.20.6-bullseye AS builder
LABEL maintainer="Siddhartha Basu <siddhartha-basu@northwestern.edu>"
ENV GOPROXY https://proxy.golang.org
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64
RUN apt-get -qq update \
    && apt-get -yqq install upx
RUN mkdir -p /modware-annotation
WORKDIR /modware-annotation
COPY go.mod ./
COPY go.sum ./
RUN go mod download
ADD cmd cmd
ADD internal internal
RUN go build \
    -a \
    -ldflags "-s -w -extldflags '-static'" \
    -installsuffix cgo \
    -tags netgo \
    -o /bin/app \
    cmd/modware-annotation/main.go
RUN upx -q -9 /bin/app

FROM gcr.io/distroless/static
COPY --from=builder /bin/app /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/app"]
