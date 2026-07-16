# flink-testkit examples

Consumer-style sample: a small Flink Kafka job under `src/main`, tested with
flink-testkit + MiniCluster under `src/test`.

This module is **not published** to Maven Central. It shows the POM shape a
real project would use.

```bash
# from repo root
mvn -pl examples -am test
```

See `examples/pom.xml` for the dependency layout (`flink-testkit-*` at
`test` scope alongside Flink / Testcontainers / JUnit).
