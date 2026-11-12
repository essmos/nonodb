# syntax=docker/dockerfile:1

FROM node:22-slim AS gui
WORKDIR /src/gui
COPY gui/package.json gui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY gui ./
RUN npm run build

FROM rust:1-slim-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY nonodb-auth ./nonodb-auth
COPY nonodb-core ./nonodb-core
COPY nonodb-daemon ./nonodb-daemon
COPY nonodb-cli ./nonodb-cli
COPY nonodb-client ./nonodb-client
COPY --from=gui /src/gui/dist ./gui/dist
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
EXPOSE 8787
VOLUME ["/var/lib/nonodb"]
# Gniazdo jest plikiem w /run/nonodb — montuj katalog, nie plik.
CMD ["/usr/local/bin/nonodb-daemon"]
