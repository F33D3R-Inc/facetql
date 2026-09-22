# facetql — the engine, as one container.
#
#   docker build -t facetql:dev .
#   docker run --rm -p 8080:8080 \
#     -e FACETQL_ENV=development -e FACETQL_ALLOW_PLAINTEXT=1 \
#     -e FACETQL_TOKENS=dev-token:dev:admin \
#     -v facetql-data:/data \
#     facetql:dev
#
# Same shape as ../fabric/Dockerfile, for the same reasons: the runtime layer
# is distroless (no shell, no package manager, non-root uid 65532), and there
# is no HEALTHCHECK because that would need a program in the image to run —
# GET / is unauthenticated and answers the liveness question; point an
# orchestrator's HTTP probe at it.

FROM rust:1-slim-bookworm AS build

# openssl-sys is built with `vendored`, which compiles OpenSSL from source
# and needs perl + make; pkg-config is what the crate probes with first.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential perl pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked --bin facetql

# The data directory the runtime image will own. Created here (the runtime
# image has no shell to mkdir with) and copied across with the runtime uid.
RUN mkdir -p /data

# distroless/cc: glibc and CA certificates, no shell and no package manager.
# `cc` rather than `static` because the binary is dynamically linked against
# the builder's libc; `:nonroot` runs as uid 65532.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /src/target/release/facetql /usr/local/bin/facetql
COPY --from=build --chown=nonroot:nonroot /data /data

# Rows live here, not in the nonroot user's home (~/.facetql), so a volume
# mounted at /data is the whole persistent state of the container.
ENV FACETQL_DATA_DIR=/data
VOLUME ["/data"]

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/facetql", "start"]
