# Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
# All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:latest AS planner
COPY . .
RUN just chef-prepare

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:latest AS base
COPY --from=planner /restate/recipe.json recipe.json
COPY justfile justfile

# avoid sharing sccache port between multiplatform builds - they share a network but not a filesystem, so it won't work
FROM base AS base-amd64
ARG SCCACHE_SERVER_PORT=4226

FROM base AS base-arm64
ARG SCCACHE_SERVER_PORT=4227

FROM base-$TARGETARCH AS builder
ARG SCCACHE_SERVER_PORT
ARG TARGETARCH

ENV RUSTC_WRAPPER=/usr/bin/sccache
ENV SCCACHE_DIR=/var/cache/sccache

# Overrides the behaviour of the release profile re including debug symbols, which in our repo is not to include them.
# Should be set to 'false' or 'true'. See https://doc.rust-lang.org/cargo/reference/environment-variables.html
ARG CARGO_PROFILE_RELEASE_DEBUG=false
ARG RESTATE_FEATURES=''
RUN just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook --bin restate-server
COPY . .
# Mount the sccache directory as a cache to leverage sccache during build
# Caching layer if nothing has changed
# Use sccache during the main build
RUN --mount=type=cache,target=/var/cache/sccache \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --bin restate-server && \
    just notice-file && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/debug/restate-server target/restate-server

# We do not need the Rust toolchain to run the server binary!
FROM debian:bookworm-slim AS runtime
COPY --from=builder /restate/target/restate-server /usr/local/bin
COPY --from=builder /restate/NOTICE /NOTICE
COPY --from=builder /restate/LICENSE /LICENSE
# copy OS roots
COPY --from=builder /etc/ssl /etc/ssl
# useful for health checks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
WORKDIR /
ENTRYPOINT ["/usr/local/bin/restate-server"]
