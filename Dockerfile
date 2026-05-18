# One image for the API and the worker; compose picks the entrypoint.
# Node >= 23.6 runs TypeScript directly (type stripping), so there is no build step.
FROM node:24-slim
ENV NODE_ENV=production
RUN corepack enable
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/shared/package.json packages/shared/
COPY apps/api/package.json apps/api/
COPY apps/worker/package.json apps/worker/
RUN pnpm install --frozen-lockfile --prod --filter '@synapse/api...' --filter '@synapse/worker...'
COPY packages/shared/src packages/shared/src
COPY apps/api/src apps/api/src
COPY apps/worker/src apps/worker/src
USER node
CMD ["node", "apps/api/src/index.ts"]
