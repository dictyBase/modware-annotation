FROM golang:1.11.10-alpine3.9
LABEL maintainer="Siddhartha Basu <siddhartha-basu@northwestern.edu>"
RUN apk add --no-cache git build-base
RUN mkdir -p /modware-annotation
WORKDIR /modware-annotation
ADD cmd cmd
ADD internal internal
COPY go.mod go.sum ./
RUN go get ./...
RUN go build -o app cmd/modware-annotation/main.go

FROM alpine:3.7
RUN apk --no-cache add ca-certificates
COPY --from=0 /modware-annotation/app /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/app"]
