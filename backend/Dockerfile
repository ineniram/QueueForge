FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker

FROM alpine:3.23
RUN adduser -D -H queueforge
COPY --from=build /out/ /usr/local/bin/
USER queueforge
CMD ["api"]
