FROM node:22-bookworm-slim@sha256:f32b81066cde10a75dbac96646099533316d94bac4150c55da1636e1f0ffdc46 AS build
WORKDIR /workspace
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY tsconfig.base.json ./
COPY contracts/trace/v1/semantic-trace-manifest.schema.json ./contracts/trace/v1/semantic-trace-manifest.schema.json
COPY contracts/trace/v1/semantic-trace-record.schema.json ./contracts/trace/v1/semantic-trace-record.schema.json
COPY contracts/trace/v2/semantic-trace-manifest.schema.json ./contracts/trace/v2/semantic-trace-manifest.schema.json
COPY packages/cloud-ingest/package.json ./packages/cloud-ingest/package.json
RUN corepack enable \
  && corepack prepare pnpm@8.15.0 --activate \
  && pnpm install --frozen-lockfile --filter @semantic-layer/cloud-ingest... --ignore-scripts
COPY packages/cloud-ingest/tsconfig.json ./packages/cloud-ingest/tsconfig.json
COPY packages/cloud-ingest/src ./packages/cloud-ingest/src
RUN pnpm --filter @semantic-layer/cloud-ingest build \
  && pnpm --filter @semantic-layer/cloud-ingest deploy --prod /app

FROM node:22-bookworm-slim@sha256:f32b81066cde10a75dbac96646099533316d94bac4150c55da1636e1f0ffdc46
ENV NODE_ENV=production
WORKDIR /workspace
COPY --from=build /workspace/contracts ./contracts
COPY --from=build /app ./packages/cloud-ingest
USER node
CMD ["node", "packages/cloud-ingest/dist/server.js"]
