# Synapse

**Real-time, event-driven notifications with delivery guarantees you can reason about.**

Backend services publish domain events (`order.shipped`, `payment.failed`, …). Synapse works out
who should hear about each event and on which channels, renders the message, and delivers it:
live in-app over WebSocket, by email, and to HMAC-signed webhooks. It respects each user's
preferences, quiet hours and rate limits, retries failures with backoff, and parks anything
undeliverable in a dead-letter queue you can replay. A worker can be killed mid-delivery without
losing or duplicating a notification, and there is a test that does exactly that.

![Synapse console](docs/console.png)

> _Demo GIF placeholder: producer panel firing an event → signal pulse → notification landing in the live inbox._

## Contents

- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Why Redis Streams](#why-redis-streams-and-not-kafka)
- [Delivery guarantees](#delivery-guarantees)
- [API reference](#api-reference)
- [Observability](#observability)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [Configuration](#configuration)
- [Repository layout](#repository-layout)
- [Future work](#future-work)

## Quick start

Requires Docker with Compose.

```sh
docker compose up --build
```

| What                       | Where                         |
| -------------------------- | ----------------------------- |
| Console (producer + inbox) | http://localhost:8080         |
| API (two replicas)         | http://localhost:3000, :3001  |
| Mailpit (caught email)     | http://localhost:8025         |
| Prometheus metrics (API)   | http://localhost:8080/metrics |

That starts Postgres, Redis, Mailpit, **two API replicas**, **two workers** and the web console.
nginx in the web container round-robins across the API replicas, so the producer's `POST` and your
WebSocket usually land on different instances. Redis pub/sub is what gets the notification across.

If those host ports are taken, override them: `WEB_PORT=8088 API_PORTS=3100-3101 docker compose up`.

Try it:

1. Click **Send event**. The notification appears in the inbox on the right, and the email appears in Mailpit.
2. Tick **Reuse the last event ID** and send again. Nothing new arrives: the duplicate is dropped.
3. Under Preferences, enable **Webhook** and save. Tick **Make the demo webhook endpoint fail**, then send a
   `payment.failed` event. Watch the webhook row retry and then land in **Dead letters**. Untick the
   failure switch and press **Replay**.

Or from a terminal:

```sh
curl -X POST localhost:3000/v1/events -H 'content-type: application/json' -d '{
  "id": "evt-123",
  "type": "order.shipped",
  "data": { "userId": "alice", "orderId": "ORD-1042", "carrier": "DHL",
            "trackingUrl": "https://track.example.com/ORD-1042" }
}'
```

### Local development

Node ≥ 24 and pnpm 10. Node runs the TypeScript sources directly (type stripping), so the API and
worker have no build step.

```sh
pnpm install
docker compose up -d postgres redis mailpit
pnpm dev:api      # :3000
pnpm dev:worker
pnpm dev:web      # :5173, proxies /v1 and the WebSocket to :3000
```

## Architecture

```mermaid
flowchart LR
  P[Producers<br/>backend services] -->|POST /v1/events| API

  subgraph API["API (Fastify) × N"]
    ING[ingest + zod validation]
    WS[WebSocket hub]
    REST[prefs · inbox · DLQ · status]
  end

  ING -->|XADD| ES[(synapse:events<br/>stream)]

  subgraph W["Worker × N"]
    R[Router stage<br/>group: router]
    D[Delivery stage<br/>group: delivery]
    PR[Delayed promoter]
  end

  ES --> R
  R -->|dedup insert| PG[(PostgreSQL<br/>events · deliveries · preferences)]
  R -->|XADD| DS[(synapse:deliveries<br/>stream)]
  DS --> D
  D -->|status| PG
  D -->|retry / quiet hours / rate limit| Z[(synapse:delayed<br/>sorted set)]
  Z --> PR -->|XADD when due| DS
  D -->|exhausted| DLQ[(synapse:dlq<br/>stream)]
  D -->|PUBLISH user channel| PS{{Redis pub/sub}}
  PS --> WS -->|push| U[Browser inbox]
  D -->|SMTP| M[Mailpit]
  D -->|HMAC-signed POST| H[Customer webhook]
  REST -->|replay| DLQ
```

There are two stages, each a Redis Streams consumer group, so each scales horizontally on its own.
Both work a whole stream batch (up to 50 entries) at a time and commit once per batch. On durable
storage, commit latency is the dominant cost, so this is where throughput comes from (see
[Benchmarks](#benchmarks)).

- **Router.** Validates the event against the catalog and resolves recipients. It drops channels the user
  opted out of (or can't be reached on), renders the template, and writes one `deliveries` row per
  (event, user, channel).
- **Delivery.** Applies quiet hours and the per-user rate limit, sends on the channel, records status, and
  schedules retries or dead-letters.

The catalog of event types, templates, recipients and channels lives in
[`packages/shared/src/catalog.ts`](packages/shared/src/catalog.ts). Adding an event type means adding one
entry with a zod schema, a template and a recipient function.

### Event flow

```mermaid
sequenceDiagram
  autonumber
  participant P as Producer
  participant A as API instance A
  participant S as Redis Streams
  participant W as Worker
  participant DB as PostgreSQL
  participant B as API instance B
  participant U as Browser (socket on B)

  P->>A: POST /v1/events {id, type, data}
  A->>A: zod-validate envelope + payload
  A->>S: XADD synapse:events
  A-->>P: 202 Accepted
  S->>W: XREADGROUP (router)
  W->>DB: INSERT events + deliveries ON CONFLICT (event_id,user_id,channel)
  W->>S: XADD synapse:deliveries (one per queued row)
  W->>S: XACK events
  S->>W: XREADGROUP (delivery)
  W->>DB: BEGIN, UPDATE status='sent' WHERE status='queued'
  W->>B: PUBLISH synapse:user:alice
  W->>DB: COMMIT
  W->>S: XACK deliveries
  B->>U: {"type":"notification", …}
  U->>B: {"type":"ack", id}
  B->>DB: status = 'delivered'
```

## Why Redis Streams (and not Kafka)

Both would work. Streams won on fit:

- **Redis is already required.** WebSocket fan-out across API instances needs pub/sub, and the rate limiter
  needs atomic counters. Streams give a durable log with consumer groups on the same infrastructure,
  so there is one fewer stateful system to run, monitor and secure.
- **The semantics match the job.** Consumer groups, per-entry acknowledgement, a pending-entries list and
  `XAUTOCLAIM` are exactly what at-least-once delivery with crash recovery needs. Kafka's model is
  per-partition offsets, which makes one slow webhook block its partition unless you add a retry-topic
  ladder. With Streams, a failing delivery is acked and parked in a delayed set without blocking anything.
- **Delays are cheap.** Backoff, quiet hours and rate-limit deferral all go through one sorted set
  scored by due time. Kafka has no native delayed delivery.
- **Scale.** Notification volume is modest next to what a single Redis handles. In the benchmarks, Redis
  absorbed 8k events/s of ingest while Postgres commits set the delivery rate (see [Benchmarks](#benchmarks)).

What Kafka would buy: long retention and replay of the raw event log, partition-level ordering, and
throughput far beyond a single Redis primary. If Synapse became the company-wide event backbone rather
than a notification service, that trade would flip. Redis runs with AOF enabled here so stream
contents survive a restart.

## Delivery guarantees

**At-least-once processing with idempotent effects.** Every stream entry is acknowledged only after its
effect is durable. A crashed worker leaves entries in the group's pending list, and a surviving worker
reclaims them with `XAUTOCLAIM` once they have been idle for `CLAIM_IDLE_MS`. Redelivery therefore
happens by design, and every step is idempotent:

| Step                                            | Idempotency mechanism                                                                                                                                                                                                                                                                                                                               |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Producer retries / republishes                  | `UNIQUE (event_id, user_id, channel)` on `deliveries`: one notification per event per user per channel, whatever the publish count.                                                                                                                                                                                                                 |
| Router redelivered after commit, before enqueue | The insert returns existing rows too (`ON CONFLICT DO UPDATE … RETURNING`), and anything still `queued` is re-enqueued.                                                                                                                                                                                                                             |
| Same delivery enqueued twice                    | The delivery stage skips anything not `queued`.                                                                                                                                                                                                                                                                                                     |
| In-app push                                     | `UPDATE … SET status='sent' WHERE status='queued'` (one statement per stream batch) is the claim; only rows this worker flipped are published. The publish happens **inside** that transaction, so a crash after commit can't skip the push, and a crash between publish and commit re-pushes the **same notification id**, which clients collapse. |
| Email / webhook                                 | A lease (`lease_until`) guarantees at most one in-flight attempt per delivery. Because the remote side effect can't be rolled back, a crash after the send but before the status write can cause a resend. Every email carries a stable `Message-ID` and every webhook an `x-synapse-delivery` header, so receivers can deduplicate.                |
| DLQ                                             | The entry is written before the row is marked `failed` (crash → a duplicate DLQ entry, never a missing one). Replay only resets rows that are still `failed`, so replaying twice is a no-op.                                                                                                                                                        |

**Retries.** Failed attempts are retried with exponential backoff and _equal jitter_: half the window is fixed
and half random, so retries never hammer a failing endpoint and workers don't synchronise. After
`MAX_ATTEMPTS` the delivery is marked `failed` and appended to `synapse:dlq`. `POST /v1/dlq/replay` requeues it
with a fresh retry budget.

**Deferral is not failure.** Quiet hours (email and webhook only; in-app is silent) and the per-user rate
limit (fixed one-minute window, shared across channels) postpone a delivery through the delayed set
without consuming retries. A Lua script moves due entries back onto the stream atomically.

**Status lifecycle** (stored in PostgreSQL):

```
queued ──► sent ──► delivered      in-app: sent = pushed, delivered = client acked over the socket
   │                                 email: sent = accepted by SMTP (no receipt exists)
   │                               webhook: delivered = 2xx from the receiver
   └─(retries exhausted)─► failed ──(replay)──► queued
```

**Proven, not claimed.** [`tests/integration/crash.test.ts`](tests/integration/crash.test.ts) runs the real
worker binary as a child process and streams 400 events at it. It `SIGKILL`s the worker once a quarter have
been delivered, and asserts that the kill landed mid-stream (some sent, some still unacknowledged). A
survivor then takes over, and the test checks that the user's live inbox received every event exactly
once and that Postgres holds exactly one row per event. A second test does the same with `SIGTERM` and
asserts a clean drain with exit code 0.

## API reference

All bodies are JSON and validated with zod; invalid input returns `400` with the zod issues.

| Method | Path                                                      | Purpose                                                                                            |
| ------ | --------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `POST` | `/v1/events`                                              | Ingest a domain event. `202` on accept, `422` for an unknown type.                                 |
| `GET`  | `/v1/event-types`                                         | The catalog: types, channels, templates and sample payloads.                                       |
| `GET`  | `/v1/users/:userId/preferences`                           | Current preferences (defaults if never set).                                                       |
| `PUT`  | `/v1/users/:userId/preferences`                           | Partial update. Setting `webhookUrl` without a secret mints one and returns it once.               |
| `GET`  | `/v1/users/:userId/notifications?limit=`                  | In-app inbox (newest first).                                                                       |
| `GET`  | `/v1/deliveries?userId=&eventId=&channel=&status=&limit=` | Delivery status tracking across channels.                                                          |
| `GET`  | `/v1/deliveries/stats`                                    | Counts by channel and status.                                                                      |
| `GET`  | `/v1/dlq`                                                 | Dead-letter entries with their delivery details.                                                   |
| `POST` | `/v1/dlq/replay`                                          | `{ "entryIds"?: string[] }`. Omit to replay everything.                                            |
| `GET`  | `/v1/ws?userId=`                                          | WebSocket. Server sends `{type:"notification", notification}`; client may send `{type:"ack", id}`. |
| `GET`  | `/health` · `/ready` · `/metrics`                         | Liveness, readiness (Postgres + Redis, fails while draining), Prometheus.                          |
| `*`    | `/demo/webhook-sink…`                                     | Built-in receiver that verifies signatures, with a failure switch for the demo.                    |

**Event envelope**

```jsonc
{
  "id": "evt-123", // producer-assigned, globally unique: the dedup key
  "type": "comment.mentioned",
  "data": { "userIds": ["alice", "bob"], "author": "carol", "snippet": "…" },
  "occurredAt": "2026-09-27T10:00:00Z", // optional
}
```

**Preferences**

```json
{
  "channels": { "in_app": true, "email": true, "webhook": false },
  "quietHours": { "start": "22:00", "end": "07:00", "timezone": "Europe/Berlin" },
  "email": "alice@example.com",
  "webhookUrl": "https://example.com/hooks/synapse",
  "webhookSecret": "at-least-16-characters"
}
```

**Webhook signature.** Each webhook carries the header
`x-synapse-signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<raw body>")>`. Verify it with a
constant-time compare and reject timestamps older than five minutes; see
[`packages/shared/src/hmac.ts`](packages/shared/src/hmac.ts). Deduplicate with `x-synapse-delivery`.

## Observability

- **Logs.** Structured JSON via pino (Fastify's logger in the API).
- **Metrics.** Prometheus: the API on `/metrics`, workers on `:9100/metrics`.

  | Metric                                      | Type      | Meaning                                    |
  | ------------------------------------------- | --------- | ------------------------------------------ |
  | `synapse_events_ingested_total{type}`       | counter   | `rate()` gives events/sec                  |
  | `synapse_events_routed_total{type}`         | counter   | Events fanned out by the router            |
  | `synapse_deliveries_total{channel,outcome}` | counter   | sent / delivered / retry / dead / deferred |
  | `synapse_delivery_latency_seconds{channel}` | histogram | Ingestion to successful hand-off           |
  | `synapse_delivery_retries_total{channel}`   | counter   | Failed attempts scheduled for retry        |
  | `synapse_dlq_size`                          | gauge     | Current DLQ length                         |
  | `synapse_ws_connections`                    | gauge     | Open sockets per API instance              |
  | `synapse_http_request_duration_seconds`     | histogram | API latency by route                       |

- **Health.** The API has `/health` (liveness) and `/ready` (dependencies reachable, not draining); workers
  expose `/health` on the metrics port. Compose healthchecks use these.
- **Graceful shutdown.** On `SIGTERM` the API fails readiness, stops accepting connections, drains in-flight
  requests and closes sockets. The worker stops reading, finishes its current batch and exits. Anything
  unacknowledged is reclaimed by peers.

## Testing

```sh
pnpm test                 # unit: templating, backoff, HMAC, quiet hours, catalog
docker compose up -d postgres redis mailpit
pnpm test:integration     # real Redis/Postgres/Mailpit, in-process API ×2 and worker processes
```

The integration suite covers ingestion validation, end-to-end in-app and email delivery with status
moving to `delivered` on ack, and cross-instance fan-out. It checks dedup under republishing, channel
opt-out, quiet-hours deferral, rate limiting that defers rather than drops, and webhook signing with
retries → DLQ → replay. The crash tests are described [above](#delivery-guarantees). CI runs lint,
typecheck, both suites and a Docker build on every push.

## Benchmarks

Measured with [`loadtest/run.ts`](loadtest/run.ts): [autocannon](https://github.com/mcollina/autocannon)
drives `POST /v1/events` through nginx, which round-robins across both API replicas. The script records the exact
event ids the API acknowledged with `202`, waits until both consumer groups and the delayed set are
empty, then checks in Postgres that **every acknowledged id has exactly one in-app notification**.
End-to-end latency is `sent_at − received_at` per notification. Raw results are in
[`loadtest/results/`](loadtest/results).

**Setup.** One laptop (AMD Ryzen 7 7730U, 8 cores / 16 threads, 15 GB), running everything in Docker Compose:
two API replicas, two workers, Postgres 17, Redis 8. Payload: `order.delivered` spread over 2,000 users,
in-app channel only (so Mailpit's SMTP speed isn't what's being measured), rate limit raised. The
machine was also running unrelated workloads (1-minute load average 3–7 at the start of each run),
and its Docker disk (btrfs, 99% full) sustains only **16–50 fsyncs/s** (`pg_test_fsync`), so every
durable commit waits tens of milliseconds. Treat these as a floor, not a ceiling.

**Saturation:** 100 connections, 20 s, as fast as possible.

| Trial | Events accepted | Ingest rate | Ingest p50 / p99 | Pipeline throughput | Backlog cleared after ingest stopped | Lost / duplicated |
| ----- | --------------- | ----------- | ---------------- | ------------------- | ------------------------------------ | ----------------- |
| 1     | 140,252         | 7,013 req/s | 16 / 48 ms       | 5,015 notif/s       | 8.1 s                                | 0 / 0             |
| 2     | 154,461         | 7,724 req/s | 14 / 47 ms       | 3,388 notif/s       | 25.8 s                               | 0 / 0             |
| 3     | 160,654         | 8,033 req/s | 14 / 45 ms       | 3,277 notif/s       | 29.1 s                               | 0 / 0             |

Ingestion never touches Postgres (it only validates and `XADD`s), so it outruns delivery and the stream
absorbs the difference. That is the point of the broker. Under saturation, end-to-end latency is
queueing time (p50 3.8–27.7 s, depending on the size of the backlog), so it isn't a useful latency figure.

**Steady load:** capped at 1,000 events/s, 50 connections, 20 s.

| Trial | Events accepted | Ingest p50 / p99 | Pipeline throughput | End-to-end p50 / p95 / p99 | Lost / duplicated |
| ----- | --------------- | ---------------- | ------------------- | -------------------------- | ----------------- |
| 1     | 20,046          | 4 / 20 ms        | 953 notif/s         | 133 / 1,702 / 2,371 ms     | 0 / 0             |
| 2     | 20,022          | 4 / 17 ms        | 997 notif/s         | 177 / 725 / 821 ms         | 0 / 0             |
| 3     | 20,018          | 4 / 20 ms        | 997 notif/s         | 48 / 75 / 86 ms            | 0 / 0             |

The pipeline keeps up with 1,000 events/s. Tail latency varied a lot between trials. Trial 1 ran
straight after the saturation runs and a truncate of the previous run's 160k rows, and all three shared
the machine with other work. The quietest run (trial 3) is the best indication of the design's own latency, but it is one
sample. **Across all six trials, 515,453 events were acknowledged and every one was delivered exactly once.**

_What moved the needle:_ the first version committed once per notification and delivered 300–390/s on this disk.
Committing once per stream batch (router: one multi-row statement; delivery: one claim-and-publish
transaction) raised that to 3.3–5.0k/s with the same guarantees.

Reproduce:

```sh
RATE_LIMIT_PER_MINUTE=1000000 docker compose up -d --build
TARGET=http://localhost:8080 pnpm loadtest                 # saturation
RATE=1000 CONNECTIONS=50 TARGET=http://localhost:8080 pnpm loadtest
```

## Configuration

Environment variables (validated with zod at boot):

| Variable                                | Default                                                 |                                                                                                         |
| --------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`                          | `postgres://synapse:synapse@localhost:5432/synapse`     |                                                                                                         |
| `REDIS_URL`                             | `redis://localhost:6379`                                |                                                                                                         |
| `PORT` / `METRICS_PORT`                 | `3000` / `9100`                                         | API port / worker metrics port                                                                          |
| `SMTP_HOST` / `SMTP_PORT` / `MAIL_FROM` | `localhost` / `1025` / `Synapse <notify@synapse.local>` |                                                                                                         |
| `MAX_ATTEMPTS`                          | `5`                                                     | Attempts before dead-lettering                                                                          |
| `BACKOFF_BASE_MS` / `BACKOFF_MAX_MS`    | `500` / `60000`                                         | Backoff window                                                                                          |
| `RATE_LIMIT_PER_MINUTE`                 | `60`                                                    | Per user, across channels                                                                               |
| `CLAIM_IDLE_MS`                         | `30000`                                                 | Reclaim threshold for a dead worker's entries; also the send lease. Keep it above `WEBHOOK_TIMEOUT_MS`. |
| `WEBHOOK_TIMEOUT_MS`                    | `5000`                                                  |                                                                                                         |
| `LOG_LEVEL`                             | `info`                                                  |                                                                                                         |

## Repository layout

```
apps/
  api/        Fastify: ingestion, preferences, inbox, status, DLQ, WebSocket hub, metrics
  worker/     Router + delivery consumers, channels, retry promoter, metrics/health server
  web/        React + Vite console (served by nginx in Docker)
packages/
  shared/     Config, catalog/templates, zod schemas, Postgres schema, Redis keys, backoff, HMAC, quiet hours
tests/
  integration/  Real-infrastructure tests, including the crash tests
loadtest/     autocannon ingest + end-to-end drain measurement
```

## Future work

- **Authentication.** Every endpoint and the WebSocket currently trust the `userId` they are given. Put JWT
  verification in front of `/v1/users/*` and the socket before exposing this beyond a trusted network.
- **Data lifecycle.** Partition `deliveries` by month and archive old partitions; the table grows without bound today.
- **Stream retention.** Streams are never trimmed. Add a periodic `XTRIM MINID` below the oldest pending entry.
- **Per-tenant fairness.** Separate streams, or weighted consumers, so one noisy producer can't starve others.
- **More channels.** Mobile push (APNs/FCM) and SMS fit the existing channel interface.
- **Digesting.** Collapse bursts (e.g. twenty mentions in a minute) into one notification instead of deferring them.
- **Template management.** Store templates in the database with versioning and localisation, rather than in code.
