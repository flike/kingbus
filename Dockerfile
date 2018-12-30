# Builder image
FROM golang:1.11.2-alpine as builder

RUN apk add --no-cache \
    make \
    git

COPY . /go/src/github.com/flike/kingbus

WORKDIR /go/src/github.com/flike/kingbus/

RUN make

FROM scratch

COPY --from=builder /go/src/github.com/flike/kingbus/build/bin/kingbus /kingbus

WORKDIR /

EXPOSE 5000

ENTRYPOINT ["/kingbus"]