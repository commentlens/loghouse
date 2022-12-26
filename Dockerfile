FROM golang:1.19-alpine AS build

WORKDIR /go/src/github.com/commentlens/loghouse/
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -o /tmp ./cmd/loghouse

FROM alpine
RUN apk add --no-cache dumb-init
COPY --from=build /tmp/loghouse /usr/bin/

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/usr/bin/loghouse"]
