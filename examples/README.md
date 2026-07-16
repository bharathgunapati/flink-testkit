# flink-testkit examples

Consumer-style sample Flink jobs under `src/main`, tested with flink-testkit
+ MiniCluster under `src/test`. These modules are **not published** to Maven
Central — they show the POM shape a real project would use.

| Module | Cases | Harness deps (`test` scope) |
|---|---|---|
| [`kafka/`](kafka/) | Enrichment sink; DLQ / poison routing | `flink-testkit-kafka-core` |
| [`jdbc/`](jdbc/) | Kafka → Postgres sink; JDBC seed/`awaitRows` only | `flink-testkit-kafka-core` + `flink-testkit-jdbc` |
| [`http/`](http/) | Apache `HttpSink`; HTTP lookup enrichment | `flink-testkit-http` |

HTTP uses Flink 2.2 + `flink-connector-http`. Kafka/JDBC use the parent Flink
profile (`1.20` by default).

```bash
# from repo root — all consumer examples (+ harness deps)
mvn -pl examples/kafka,examples/jdbc,examples/http -am test

# one connector at a time
mvn -pl examples/kafka -am test
mvn -pl examples/jdbc -am test
mvn -pl examples/http -am test
```
