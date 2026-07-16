# flink-testkit — Roadmap

Kafka is the reference module. Next harnesses follow the same contract
pattern for other Flink connectors. Fluss/Paimon/savepoint work is
explicitly deferred.

---

## Shipped — Kafka (Phases 1–3.5)

- `FlinkTestExtension` (in `flink-testkit-core`) + `@KafkaTopic` + `TopicHandle`
- JSON, keys/headers, Avro + Schema Registry, DLQ / poison payloads
- Modules: `flink-testkit-core`, `flink-testkit-kafka-core`, `flink-testkit-kafka-avro`

---

## Phase JDBC — Postgres harness *(shipped MVP)*

**Goal:** same thin harness for Flink JDBC jobs against a real Postgres.

- `@JdbcTable` + `TableHandle<T>` on the shared `FlinkTestExtension`
- Shared Postgres container (lazy, JVM-wide)
- Typed `insert` / `awaitRows` (condition-based, no fixed sleep)
- Job config via `jdbcUrl()` / `username()` / `password()` / `tableName()`
- SPI composition so modules stay free of each other's deps
- Example: Kafka → JDBC sink

**Out of scope for this phase:** MySQL/other dialects, Flyway, XA/exactly-once edge cases

---

## Phase HTTP — Apache Flink HTTP connector *(shipped MVP)*

**Goal:** same pattern for [flink-connector-http](https://github.com/apache/flink-connector-http).

- `@HttpEndpoint` + `HttpHandle`
- MockServer **container** (shared, lazy)
- Stub responses + `awaitRequests` (incl. query params on recorded requests)
- Job config via `baseUrl()` / `url()`
- Discovered by `FlinkTestExtension` via SPI
- Examples on Flink 2.2 + `flink-connector-http` 1.0.0-2.2:
  - Apache `HttpSink`
  - HTTP lookup source (`connector = 'http'`) enrichment / temporal join

---

## Extension model

One public entry point: `@ExtendWith(FlinkTestExtension.class)` from
`flink-testkit-core`.

Connector modules (`kafka-core`, `jdbc`, `http`) each depend on `core`
only — not on each other — and plug in via SPI (`KafkaTopicPlugin`,
`JdbcTablePlugin`, `HttpEndpointPlugin`).
Users declare `@KafkaTopic` / `@JdbcTable` / `@HttpEndpoint`; they do not pick
per-connector extensions.

---

## Explicitly deferred

- Fluss / Paimon harnesses
- Savepoint/restore helpers
- Exactly-once / rebalance / compaction Kafka depth work
- Batch-retry orchestration

---

## Sequencing

| Phase | Theme | Status |
|---|---|---|
| Kafka 1–3.5 | JSON → keys → Avro → DLQ | Shipped |
| **JDBC** | Postgres `@JdbcTable` / `TableHandle` | Shipped (MVP) |
| **HTTP** | MockServer + Apache `flink-connector-http` | Sink + lookup MVP |
| **CI matrix** | JDK 17/21 × Flink profiles 1.19/1.20 | Shipped |
| Deferred | Fluss, Paimon, savepoints, deep Kafka | Later |

**Guiding principle:** each phase independently shippable and useful.
