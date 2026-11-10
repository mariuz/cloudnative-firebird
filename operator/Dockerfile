# Build stage
FROM node:22-alpine AS builder

WORKDIR /app

# Copy package files and install dependencies
COPY package.json package-lock.json* ./
RUN npm ci --ignore-scripts

# Copy source and build
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# The instance pods' agent (docs/rust-agent-roadmap.md): one static binary, compiled on the build
# platform for the target one (no emulation), linked with the toolchain's own linker
FROM --platform=$BUILDPLATFORM rust:1.97-alpine AS agent
ARG TARGETARCH
WORKDIR /agent
COPY agent ./
RUN case "$TARGETARCH" in arm64) target=aarch64-unknown-linux-musl ;; *) target=x86_64-unknown-linux-musl ;; esac && \
    rustup target add "$target" && \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
      cargo build --release --locked --target "$target" && \
    cp "target/$target/release/fb-agent" /fb-agent

# Production stage
FROM node:22-alpine AS runner

# Create non-root user
RUN addgroup -g 65532 nonroot && adduser -u 65532 -G nonroot -s /bin/sh -D nonroot

WORKDIR /app

# Copy only production dependencies
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force

# Copy built files
COPY --from=builder /app/dist ./dist
# Replication scripts are shipped to instance pods via the cluster ConfigMap
COPY --from=builder /app/src/replication ./dist/replication
# Copied into the instance pods that opt in (utils/agent.ts)
COPY --from=agent /fb-agent ./fb-agent

# Use non-root user
USER nonroot

ENV NODE_ENV=production

ENTRYPOINT ["node", "dist/index.js"]
