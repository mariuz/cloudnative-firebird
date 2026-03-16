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

# Use non-root user
USER nonroot

ENV NODE_ENV=production

ENTRYPOINT ["node", "dist/index.js"]
