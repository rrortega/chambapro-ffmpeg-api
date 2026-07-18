# Stage 1: Chef
FROM rust:slim-bullseye AS chef
RUN apt-get update && apt-get install -y pkg-config libssl-dev tar gzip ca-certificates && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef
WORKDIR /app

# Stage 2: Recipe
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Builder
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin chambapro-ffmpeg-api

# Stage 4: Runtime
FROM debian:bullseye-slim AS runtime
RUN apt-get update && apt-get install -y ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/chambapro-ffmpeg-api /usr/local/bin/

ENV PORT=8080
EXPOSE 8080
ENV RUST_LOG=info

CMD ["chambapro-ffmpeg-api"]
