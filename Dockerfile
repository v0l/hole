FROM rust:trixie AS build
WORKDIR /app/src
COPY Cargo.toml .
COPY Cargo.lock .
COPY src src
RUN cargo install --path . --root /app/build

FROM debian:trixie-slim
WORKDIR /app
RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /app/build .
ENV RUST_LOG=info
ENTRYPOINT ["./bin/nostrhole"]