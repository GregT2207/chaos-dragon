FROM golang:1.26-alpine as compiler
WORKDIR /app
COPY . .
RUN go build -o chaos-dragon

FROM debian:bookworm-slim
COPY --from=compiler /app/chaos-dragon /usr/local/bin/chaos-dragon
CMD echo "" \
 && echo "==================================" \
 && echo "🐉 Chaos Dragon CLI" \
 && echo "🐲 \"chaos-dragon -h\" for instructions" \
 && echo "==================================" \