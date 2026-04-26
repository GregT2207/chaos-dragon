FROM rust:slim-trixie AS compiler
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=compiler /app/target/release/chaos-dragon /usr/local/bin/chaos-dragon
CMD ["chaos-dragon"]