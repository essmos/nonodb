# syntax=docker/dockerfile:1

FROM rust:1-slim-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY nonodb-core ./nonodb-core
COPY nonodb-daemon ./nonodb-daemon
COPY nonodb-cli ./nonodb-cli
RUN cargo build --release --bin nonodb-daemon --bin nonodb-cli

FROM debian:bookworm-slim
RUN useradd --system --home /var/lib/nonodb --shell /usr/sbin/nologin nonodb \
    && mkdir -p /var/lib/nonodb /run/nonodb \
    && chown -R nonodb:nonodb /var/lib/nonodb /run/nonodb
COPY --from=builder /src/target/release/nonodb-daemon /usr/local/bin/nonodb-daemon
COPY --from=builder /src/target/release/nonodb-cli /usr/local/bin/nonodb-cli

ENV NONODB_DATA=/var/lib/nonodb \
    NONODB_SOCK=/run/nonodb/nonodb.sock \
    RUST_LOG=info

USER nonodb
VOLUME ["/var/lib/nonodb"]
# Gniazdo jest plikiem w /run/nonodb — montuj katalog, nie plik.
CMD ["/usr/local/bin/nonodb-daemon"]
