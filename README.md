# flink-testkit (v0.0.1)

An **extremely lightweight** integration-testing harness for Flink jobs
that use Kafka, JDBC (Postgres), and HTTP connectors. Thin layer over
Testcontainers: typed produce/await-style APIs, one `FlinkTestExtension`
entry point.

## Why flink-testkit?

### How is this different from Testcontainers?

flink-testkit **uses** Testcontainers — it does not replace them.

- **Testcontainers** starts real Kafka / Postgres / MockServer in Docker.
- **flink-testkit** is the Flink-shaped layer on top: typed handles, seed +
  await helpers, and job config from the same declaration, so you are not
  re-writing topic/table wiring and `Thread.sleep` loops in every test.

If your Flink connector tests “already use Testcontainers,” they solve
infrastructure. This harness solves the **test API** — less boilerplate,
less flakiness, one place to get the schema or URL wrong.

### What you get

- **Less boilerplate** — no hand-rolled container wiring, topic/table naming,
  or consumer-group bookkeeping in every test.
- **Less flaky CI** — `produce` / `awaitRecords` / `awaitRows` / `awaitRequests`
  wait on real completion conditions, not `Thread.sleep`.
- **One handle, three jobs** — the same `@KafkaTopic` / `@JdbcTable` /
  `@HttpEndpoint` declaration seeds data, configures the Flink job, and
  asserts outcomes.
- **Thin classpath** — pull only the modules you need; containers start
  lazily on first use.
- **Real connectors** — exercise Kafka, JDBC, and HTTP the way production
  jobs do, under one `@ExtendWith(FlinkTestExtension.class)`.

See [Before / after](#before--after) for a side-by-side comparison.

## Modules (keep your classpath thin)

| Artifact | Approx. jar size | When to use |
|---|---|---|
| `flink-testkit-core` | ~10 KB | Always — hosts `FlinkTestExtension` (pulled transitively by connector modules). |
| `flink-testkit-kafka-core` | ~25 KB | Kafka — JSON, keys/headers, DLQ. **No Avro/Confluent.** |
| `flink-testkit-kafka-avro` | ~7 KB | Only if you use `@KafkaTopic(format = Format.AVRO)` |
| `flink-testkit-jdbc` | ~13 KB | Only if you use `@JdbcTable` (Postgres) |
| `flink-testkit-http` | ~12 KB | Only if you use `@HttpEndpoint` (MockServer) |

Harness jars are ~7–25 KB each. That is the thin API layer only — your tests
still bring Testcontainers / Kafka clients / drivers as usual.

Containers start **lazily**: Kafka on first `@KafkaTopic`, Schema Registry
on first Avro topic, Postgres on first `@JdbcTable`. Core runtime
dependencies are Maven `optional` (not forced onto your project — bring
JUnit / Testcontainers / drivers as you already do for Flink tests).

## Requirements

- Java 17+ (bytecode target 17; CI also runs on JDK 21)
- Maven 3.8+
- A working Docker daemon (Docker Desktop, Rancher Desktop, or Colima)

## Use it in your project

Add the modules you need from Maven Central (`0.0.1`):

**JSON Kafka (recommended default):**

```xml
<dependency>
    <groupId>io.github.bharathgunapati</groupId>
    <artifactId>flink-testkit-kafka-core</artifactId>
    <version>0.0.1</version>
    <scope>test</scope>
</dependency>
```

**Also using Avro + Schema Registry:**

```xml
<dependency>
    <groupId>io.github.bharathgunapati</groupId>
    <artifactId>flink-testkit-kafka-avro</artifactId>
    <version>0.0.1</version>
    <scope>test</scope>
</dependency>
```

**Also using JDBC (Postgres):**

```xml
<dependency>
    <groupId>io.github.bharathgunapati</groupId>
    <artifactId>flink-testkit-jdbc</artifactId>
    <version>0.0.1</version>
    <scope>test</scope>
</dependency>
```

**Also using HTTP (MockServer / Apache Flink HTTP connector):**

```xml
<dependency>
    <groupId>io.github.bharathgunapati</groupId>
    <artifactId>flink-testkit-http</artifactId>
    <version>0.0.1</version>
    <scope>test</scope>
</dependency>
```

You still bring your own Flink + connectors (and Jackson / Testcontainers /
JUnit / JDBC driver) for the job under test — harness optional deps are not
transitive on purpose.

### Consumer-style sample modules

See [`examples/`](examples/) for MiniCluster Flink jobs under `src/main` that
depend on flink-testkit with `<scope>test</scope>` the way a real project
would (not published to Maven Central):

| Module | Demo |
|---|---|
| [`examples/kafka`](examples/kafka/) | Enrichment; DLQ / poison routing |
| [`examples/jdbc`](examples/jdbc/) | Kafka → Postgres; JDBC seed/`awaitRows` |
| [`examples/http`](examples/http/) | Apache HttpSink; HTTP lookup enrichment |

```bash
mvn -pl examples/kafka,examples/jdbc,examples/http -am test
```

## Quickstart

### Kafka (JSON)

```java
@ExtendWith(FlinkTestExtension.class)
class MyJobTest {

    @KafkaTopic(valueType = OrderEvent.class)
    static TopicHandle<OrderEvent> orders;

    @KafkaTopic(valueType = EnrichedOrder.class)
    static TopicHandle<EnrichedOrder> enrichedOrders;

    @Test
    void uppercasesCustomerName() throws Exception {
        JobClient jobClient = UppercaseJob.runAsync(
            orders.bootstrapServers(), orders.topicName(), enrichedOrders.topicName());

        try {
            orders.produce(new OrderEvent("order-1", "alice", 42.50));

            List<EnrichedOrder> results = enrichedOrders.awaitRecords(
                records -> records.stream().anyMatch(r -> "order-1".equals(r.orderId())),
                Duration.ofSeconds(30));

            assertThat(results)
                .filteredOn(r -> "order-1".equals(r.orderId()))
                .extracting(EnrichedOrder::customerName)
                .containsExactly("ALICE");
        } finally {
            jobClient.cancel();
        }
    }
}
```

Avro + Schema Registry — `valueType` must be an Avro `SpecificRecord`.
Schema Registry starts automatically; pass `payments.schemaRegistryUrl()` into
the job:

```java
@KafkaTopic(valueType = PaymentEvent.class, format = Format.AVRO)
static TopicHandle<PaymentEvent> payments;
```

DLQ / poison messages:

```java
@KafkaTopic(valueType = OrderEvent.class, dlq = true)
static TopicHandle<OrderEvent> orders;

orders.produceInvalid("{not-json");
List<byte[]> poison = orders.awaitDlqRecords(1, Duration.ofSeconds(10));
// configure the job with orders.dlqTopicName()
```

### JDBC (Postgres)

```java
@JdbcTable(
    valueType = Order.class,
    ddl = """
        CREATE TABLE {table} (
          order_id VARCHAR(64) PRIMARY KEY,
          customer_name VARCHAR(128) NOT NULL,
          amount DOUBLE PRECISION NOT NULL
        )
        """)
static TableHandle<Order> orders;

orders.insert(new Order("o-1", "alice", 10.0));
List<Order> rows = orders.awaitRows(1, Duration.ofSeconds(10));
// configure the Flink JDBC sink with orders.jdbcUrl(), username(), password(), tableName()
```

Use `{table}` in `ddl()` — it is replaced with a unique, quoted table name.

### HTTP (MockServer)

```java
@HttpEndpoint(path = "/ingest")
static HttpHandle ingest;

ingest.stubPost(202);
// configure Apache HttpSink with ingest.url()
List<ReceivedRequest> hits = ingest.awaitRequests(1, Duration.ofSeconds(10));
```

Lookup / enrichment (Apache `connector = 'http'`):

```java
@HttpEndpoint(path = "/customers")
static HttpHandle customers;

customers.stubGetJson(200, "{\"customer_id\":\"alice\",\"tier\":\"gold\",\"region\":\"us-west\"}");
// configure the Flink HTTP lookup table with customers.url()
// JOIN ... FOR SYSTEM_TIME AS OF ... ON customer_id
List<ReceivedRequest> hits = customers.awaitRequests(1, Duration.ofSeconds(10));
// hits.get(0).queryParams() contains the join key
```

A unique suffix is appended to `path` for isolation. Use `url()` / `baseUrl()`
for the job under test.

### Kafka → JDBC (composed)

```java
@ExtendWith(FlinkTestExtension.class)
class FraudDetectionJobTest {

    @KafkaTopic(valueType = Transaction.class)
    static TopicHandle<Transaction> input;

    @JdbcTable(
        valueType = FraudAlert.class,
        ddl = """
            CREATE TABLE {table} (
              account_id VARCHAR(64) PRIMARY KEY,
              reason VARCHAR(256) NOT NULL
            )
            """)
    static TableHandle<FraudAlert> alerts;

    @Test
    void flagsRapidWithdrawals() throws Exception {
        // job uses input.bootstrapServers()/topicName() and alerts.jdbcUrl()/...
        input.produce(
            new Transaction("acct-6", 500, Instant.now()),
            new Transaction("acct-6", 500, Instant.now().plusSeconds(2))
        );

        List<FraudAlert> rows = alerts.awaitRows(1, Duration.ofSeconds(30));
        assertThat(rows).extracting(FraudAlert::accountId).containsExactly("acct-6");
    }
}
```

That's the whole test. No container wiring, no manual `Properties`, no
`Thread.sleep`.

Working examples live under each module's
`src/test/java/io/flinktestkit/example` — see `UppercaseJobTest` (Kafka),
`OrderJdbcSinkJobTest` (Kafka → JDBC), `OrderHttpSinkJobTest` (HTTP sink),
and `OrderHttpLookupJobTest` (HTTP lookup enrichment). For standalone
consumer POMs + MiniCluster jobs, see [`examples/`](examples/)
(Kafka / JDBC / HTTP).

**Note:** Kafka topics and JDBC tables are created once per test class. If
you have multiple `@Test` methods sharing the same handles, wait for
specific IDs with predicate-based await helpers rather than assuming an
empty store.

## Before / after

**Before** — the boilerplate this replaces, roughly:

```java
KafkaContainer kafka = new KafkaContainer("confluentinc/cp-kafka:7.6.1");
kafka.start();

AdminClient admin = AdminClient.create(adminProps(kafka));
String inputTopic = "orders-" + UUID.randomUUID();
String outputTopic = "enriched-" + UUID.randomUUID();
admin.createTopics(List.of(new NewTopic(inputTopic, 1, (short) 1),
                            new NewTopic(outputTopic, 1, (short) 1))).all().get();

KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps(kafka));
producer.send(new ProducerRecord<>(inputTopic, objectMapper.writeValueAsBytes(order))).get();

// ... submit the job, pointed at inputTopic/outputTopic ...

KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps(kafka, "test-" + UUID.randomUUID()));
consumer.subscribe(List.of(outputTopic));
Thread.sleep(5000); // <- and hope that's long enough, every time, in CI too
ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));
EnrichedOrder result = objectMapper.readValue(records.iterator().next().value(), EnrichedOrder.class);
assertThat(result.customerName()).isEqualTo("ALICE");

consumer.close();
producer.close();
admin.deleteTopics(List.of(inputTopic, outputTopic));
kafka.stop();
```

**After** — same scenario with flink-testkit:

```java
@ExtendWith(FlinkTestExtension.class)
class UppercaseJobTest {

    @KafkaTopic(valueType = OrderEvent.class)
    static TopicHandle<OrderEvent> orders;

    @KafkaTopic(valueType = EnrichedOrder.class)
    static TopicHandle<EnrichedOrder> enrichedOrders;

    @Test
    void uppercasesCustomerName() throws Exception {
        JobClient jobClient = UppercaseJob.runAsync(
            orders.bootstrapServers(), orders.topicName(), enrichedOrders.topicName());

        try {
            orders.produce(new OrderEvent("order-1", "alice", 42.50));

            List<EnrichedOrder> results = enrichedOrders.awaitRecords(
                records -> records.stream().anyMatch(r -> "order-1".equals(r.orderId())),
                Duration.ofSeconds(30));

            assertThat(results)
                .filteredOn(r -> "order-1".equals(r.orderId()))
                .extracting(EnrichedOrder::customerName)
                .containsExactly("ALICE");
        } finally {
            jobClient.cancel();
        }
    }
}
```

Same guarantees, none of the ceremony, and no fixed-delay flakiness.

## How it works

- **`FlinkTestExtension`** (in `flink-testkit-core`) composes connector
  plugins via SPI. Each plugin owns JVM-wide singleton containers (started
  once, reused across test classes, torn down via shutdown hook). Per test
  class it creates freshly named Kafka topics / Postgres tables / HTTP paths
  and injects handles before any `@Test` runs.
- **Await helpers** poll with a real completion condition (count or
  predicate) — never `Thread.sleep`, never a shared/stale consumer group.
- **`@KafkaTopic(format = Format.AVRO)`** starts a shared Schema Registry
  on first use.
- **`@KafkaTopic(dlq = true)`** creates a companion DLQ topic for poison
  routing assertions.

Example jobs cover map, keyed state, windows, multi-source join, headers,
Avro, DLQ, Kafka → JDBC, and HTTP sink/lookup. Docker needs enough
memory/CPU for the containers your tests actually touch.

## Support matrix

The published harness jars are **Flink-agnostic** (Flink is test-scoped for
examples only). CI verifies:

| JDK | Flink profile | Modules exercised |
|---|---|---|
| 17, 21 | `-Pflink-1.20` (default) | kafka / jdbc examples on Flink 1.20 |
| 17, 21 | `-Pflink-1.19` | kafka / jdbc examples on Flink 1.19 |
| 17, 21 | (always) | http examples on Flink 2.2 + `flink-connector-http` |

## Try the examples locally

```bash
git clone https://github.com/bharathgunapati/flink-testkit.git
cd flink-testkit
mvn test                  # Flink 1.20 kafka/jdbc examples (default)
mvn -Pflink-1.19 test     # Flink 1.19 kafka/jdbc examples
```

### Docker not found? (Rancher Desktop / Colima)

Testcontainers talks to the Docker socket. Rancher Desktop and Colima do
not expose `/var/run/docker.sock` by default. IntelliJ's Maven runner also
does **not** inherit exports from your shell, so you need one of the
options below.

**Option A — terminal**

```bash
# Rancher Desktop
export DOCKER_HOST=unix://$HOME/.rd/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
export TESTCONTAINERS_HOST_OVERRIDE=localhost

# Colima (typical)
# export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
# export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
# export TESTCONTAINERS_HOST_OVERRIDE=localhost

mvn test
```

**Option B — IntelliJ IDEA**

Settings → Build, Execution, Deployment → Build Tools → Maven → Runner →
**Environment variables**, then add (use your home path if `$HOME` is not
expanded):

```text
DOCKER_HOST=unix://$HOME/.rd/docker.sock
TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
TESTCONTAINERS_HOST_OVERRIDE=localhost
```

For Colima, point `DOCKER_HOST` at `$HOME/.colima/default/docker.sock`
instead. Apply, then re-run Maven (`clean install` / `test`).

**Option C — symlink once (works for terminal and IntelliJ)**

```bash
# Rancher Desktop
sudo ln -sf "$HOME/.rd/docker.sock" /var/run/docker.sock

# Colima
# sudo ln -sf "$HOME/.colima/default/docker.sock" /var/run/docker.sock
```

Docker Desktop users can usually skip all of this and run `mvn test` as-is.

## License

Apache License 2.0 (matching the Flink and Kafka ecosystems this plugs into).
