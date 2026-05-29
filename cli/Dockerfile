FROM golang:1.26-alpine as compiler
WORKDIR /app
COPY . .
RUN go build -o chaos-dragon

FROM debian:bookworm-slim
COPY --from=compiler /app/chaos-dragon /usr/local/bin/chaos-dragon