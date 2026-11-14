# syntax=docker/dockerfile:1
#
# Container image of the `squad` server.
#
#   docker build -t squad .
#   docker run -d -p 5432:5432 -v squad-data:/data -e SQUAD_PASSWORD=secret squad
#   psql -h 127.0.0.1 -U squad squad
#
# The server listens on 0.0.0.0:5432 inside the container and therefore
# refuses to start without a password (SQUAD_PASSWORD, SQUAD_PASSWORD_FILE
# or --users-file) unless `--trust` is passed explicitly. See docs/server.md.

# Builder: the MSRV toolchain plus clang, which the bundled PostgreSQL
# parser (libpg_query, compiled from C, bindings via bindgen) needs.
FROM rust:1.89.0-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# Limit parallel jobs with --build-arg JOBS=2 on small machines.
ARG JOBS
ENV CARGO_PROFILE_RELEASE_DEBUG=0
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p squad-server --bin squad ${JOBS:+--jobs "$JOBS"} \
    && cp target/release/squad /usr/local/bin/squad \
    && strip /usr/local/bin/squad

# Runtime: slim Debian, an unprivileged user, the data directory as a
# volume. The S3 client brings its own root certificates; the system
# bundle is copied for anything else that needs one.
FROM debian:bookworm-slim
RUN groupadd --system --gid 10001 squad \
    && useradd --system --uid 10001 --gid squad --home-dir /data --shell /usr/sbin/nologin squad \
    && mkdir /data \
    && chown squad:squad /data
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /usr/local/bin/squad /usr/local/bin/squad
USER squad:squad
VOLUME /data
EXPOSE 5432
# Logs in with SQUAD_PASSWORD / SQUAD_PASSWORD_FILE when set; without a
# password it still checks that the server answers.
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD ["squad", "ping", "--host", "127.0.0.1", "--port", "5432", "--timeout", "4s"]
ENTRYPOINT ["squad"]
CMD ["serve", "/data", "--listen", "0.0.0.0:5432"]
