# event-bus-service

> The owner of the Kafka topic topology of the platform: on startup it creates all 22 topics (11 business topics plus 11 `.DLT` dead-letter topics) with the right partitions, replication factor and retention, and it exposes a small inspection API under `/internal/events`. Public port **8090**, actuator port **18090**.

## Table of Contents

- [What It Does](#what-it-does)
- [Architecture At A Glance](#architecture-at-a-glance)
- [HTTP API](#http-api)
- [Error Response Shape](#error-response-shape)
- [Database](#database)
- [Kafka Topics](#kafka-topics)
- [Configuration](#configuration)
- [Behaviour Rules](#behaviour-rules)
- [Running It](#running-it)
- [API Quick Reference (curl examples)](#api-quick-reference-curl-examples)
- [Configuration Reference (quick table)](#configuration-reference-quick-table)
- [Troubleshooting](#troubleshooting)
- [Notes and Known Limitations](#notes-and-known-limitations)

## What It Does

The event bus service owns the **Kafka topic topology of the platform**. It is the only service that
talks to the Kafka **Admin API**: on startup it creates every topic of the catalog with the right
number of partitions, replication factor and retention, and it re-applies those settings on every
restart (`spring.kafka.admin.modify-topic-configs: true`), so a hand-edited topic is put back.

This matters because the Kafka broker runs with **auto-create disabled**
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` in `event-bus-service/docker-compose.yml`) — without
this service no topic exists and *every* event publish fails. `run-all.sh` says the same thing:

> `# The event bus service creates the Kafka topics. Kafka runs with`
> `# auto-create turned off, so without it no topic exists and every`
> `# event publish fails.`

Besides provisioning, it exposes a small **inspection** API (`/internal/events/**`) that reports
whether the broker is reachable, whether every catalog topic really exists with the expected
settings, and how many messages are sitting in the dead-letter topics.

It does **not** publish or consume business events — the domain services (`account-service`,
`user-service`, `otp-service`, `core-banking-service`, `charge-fee-service`,
`fund-transfer-service`, `notification-service`) do that. This service owns the *namespace*, not
the traffic.

It owns no database either: there is no datasource in this repository.

## Architecture At A Glance

```
   ┌────────────────────────────────────────────────────────────┐
   │  event-bus-service (kafka-event-service) :8090              │
   │  • KafkaAdmin.NewTopics bean → creates/repairs 22 topics    │
   │  • /internal/events/{health,topics,dead-letters}            │
   └───────────────┬──────────────────────────────┬─────────────┘
                   │ Admin API (create/describe)   │ registry
                   ▼                              ▼
        ┌──────────────────────┐        ┌────────────────────────┐
        │ Kafka (banking-kafka)│        │ discovery-service :8761│
        │ 127.0.0.1:9092       │        │ (registers as          │
        │ auto-create OFF      │        │  KAFKA-EVENT-SERVICE)   │
        └──────┬───────────────┘        └────────────────────────┘
               │ topics: account.*, fee.*, otp.*, cbs.*, transfer.*,
               │        user.*, notification.* (+ .DLT for each)
               ▼
        account-service, user-service, otp-service, core-banking-service,
        charge-fee-service, fund-transfer-service, notification-service
```

| Service | Depends on | Depended on by |
|---|---|---|
| `event-bus-service` (8090) | a reachable Kafka broker on `localhost:9092`; the discovery service (8761) for registration | nobody at runtime — it is a *startup* dependency of every service that publishes events |

## HTTP API

The controller is `src/main/java/com/banking/eventbus/controller/InternalEventBusController.java`
(`@RestController @RequestMapping("/internal/events")`). **All three endpoints are `/internal/**`
paths, so the API gateway never forwards them** — the gateway answers `404` for any `/internal/**`
request.

### GET /internal/events/health

Whether the broker is reachable and whether every catalog topic exists.

| Response field | Type | Notes |
|---|---|---|
| `brokerReachable` | boolean | `false` if the broker could not be contacted |
| `allTopicsPresent` | boolean | `true` only when no catalog topic is missing |
| `missingTopics` | array of string | The names of the catalog topics that do not exist on the broker |

| Status | When | Body |
|---|---|---|
| `200` | `brokerReachable && allTopicsPresent` | `{"brokerReachable":true,"allTopicsPresent":true,"missingTopics":[]}` |
| `503` | The broker is unreachable, or a topic is missing | `{"brokerReachable":false,"allTopicsPresent":false,"missingTopics":[]}` |

The `ready()` helper is `@JsonIgnore` and never appears in the body.

### GET /internal/events/topics

One entry per catalog topic — all **22** of them, including the `.DLT` topics.

| Response field | Type | Notes |
|---|---|---|
| `name` | string | The topic name, e.g. `account.commands` |
| `kind` | string | `COMMANDS`, `EVENTS` or `DEAD_LETTER` |
| `exists` | boolean | Whether the topic exists on the broker right now |
| `partitions` | integer | The **live** partition count; `0` when the topic does not exist |
| `retentionMs` | integer or null | The **live** `retention.ms`; `null` when the topic does not exist |
| `inSync` | boolean | `true` only when the live partition count **and** the live `retention.ms` match the catalog exactly |
| `description` | string | The description from the catalog |

| Status | When |
|---|---|
| `200` | Always (when the broker is reachable) |
| `503` | The broker is unreachable — `Kafka is not reachable` |

### GET /internal/events/dead-letters

The message count of every dead-letter topic that exists on the broker.

| Response field | Type | Notes |
|---|---|---|
| `topic` | string | The `.DLT` topic name, e.g. `user.events.DLT` |
| `messages` | integer | `latest offset − earliest offset`, summed over the partitions |

| Status | When |
|---|---|
| `200` | Always (when the broker is reachable) |
| `503` | The broker is unreachable — `Kafka is not reachable` |

If the broker is reachable but no dead-letter topic exists yet, the list is empty (`[]`).

### Error Response Shape

Every error produced by this service uses the same record, defined in
`src/main/java/com/banking/eventbus/exception/ErrorResponse.java`:

```java
record ErrorResponse(LocalDateTime timestamp, int status, String error, String message)
```

| Field | Type | Notes |
|---|---|---|
| `timestamp` | string | A **local** `LocalDateTime`, e.g. `2026-11-02T17:26:12.727697` — no timezone offset |
| `status` | integer | The HTTP status code, repeated in the body |
| `error` | string | The HTTP reason phrase, e.g. `Service Unavailable` |
| `message` | string | The human-readable message |

| Situation | Status | `error` | `message` |
|---|---|---|---|
| Broker unreachable | `503` | `Service Unavailable` | `Kafka is not reachable` |
| Unknown path | `404` | `Not Found` | `Not found` |
| Anything else | `500` | `Internal Server Error` | `Something went wrong` (the stack trace is logged) |

Example:

```json
{
  "timestamp": "2026-11-02T17:26:12.727697",
  "status": 503,
  "error": "Service Unavailable",
  "message": "Kafka is not reachable"
}
```

## Database

**Not applicable.** This service has no datasource, no JDBC driver and no entity classes. All of its
state lives in Kafka itself: the topics it creates and the messages that land in them.

## Kafka Topics

This service **creates and repairs** topics; it does not publish to them or consume from them.
The catalog lives in `src/main/java/com/banking/eventbus/catalog/TopicCatalog.java` and the names
in `src/main/java/com/banking/eventbus/catalog/TopicNames.java`.

### How the topics are created

`src/main/java/com/banking/eventbus/config/KafkaTopicsConfig.java`:

```java
@Bean
public KafkaAdmin.NewTopics bankingTopics(TopicCatalog catalog,
                                          @Value("${app.kafka.replication-factor:1}") int replicationFactor) {
    NewTopic[] topics = catalog.all().stream()
            .map(definition -> TopicBuilder.name(definition.name())
                    .partitions(definition.partitions())
                    .replicas(replicationFactor)
                    .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(definition.retentionMs()))
                    .build())
            .toArray(NewTopic[]::new);

    return new KafkaAdmin.NewTopics(topics);
}
```

Every catalog entry is created **twice**: once as the real topic and once as its dead-letter
topic, appended with the suffix `TopicNames.DLT_SUFFIX = ".DLT"`. `TopicCatalog.PARTITIONS = 3` —
*"Three partitions let up to three consumers of one service share the work."* `TopicKind` has three
values: `COMMANDS`, `EVENTS`, `DEAD_LETTER`.

**Every topic has 3 partitions and 1 replica.** The retention column is `retention.ms`.

### The complete topic list — 22 topics (11 business + 11 dead-letter)

| # | Topic | Kind | Partitions | Replicas | `retention.ms` | DLT retention | Description (from the catalog) |
|---|---|---|---|---|---|---|---|
| 1 | `account.commands` | COMMANDS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Commands to Account Service: ValidateTransfer, ValidateCashAccount` |
| 2 | `account.commands.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from account.commands that failed after all retries` |
| 3 | `account.events` | EVENTS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Facts from Account Service: TransferValidated, TransferRejected, CashAccountValidated, CashAccountRejected, AccountOpened` |
| 4 | `account.events.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from account.events that failed after all retries` |
| 5 | `fee.commands` | COMMANDS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Commands to Charge Fee Service: CalculateFee` |
| 6 | `fee.commands.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from fee.commands that failed after all retries` |
| 7 | `fee.events` | EVENTS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Facts from Charge Fee Service: FeeCalculated` |
| 8 | `fee.events.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from fee.events that failed after all retries` |
| 9 | `otp.commands` | COMMANDS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Commands to OTP Service: IssueOtp` |
| 10 | `otp.commands.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from otp.commands that failed after all retries` |
| 11 | `otp.events` | EVENTS | 3 | 1 | **7200000 (2 hours)** | **14400000 (4 hours)** | `Facts from OTP Service: OtpIssued (the code is encrypted)` |
| 12 | `otp.events.DLT` | DEAD_LETTER | 3 | 1 | 14400000 (4 hours) | – | `Messages from otp.events that failed after all retries` |
| 13 | `cbs.commands` | COMMANDS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Commands to Core Banking: CreateLedgerAccount, CheckBalance, Withdraw, Deposit, Reversal` |
| 14 | `cbs.commands.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from cbs.commands that failed after all retries` |
| 15 | `cbs.events` | EVENTS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Facts from Core Banking: LedgerAccountCreated, BalanceChecked, BalanceRejected, WithdrawCompleted, WithdrawFailed, DepositCompleted, DepositFailed, ReversalCompleted, ReversalFailed` |
| 16 | `cbs.events.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from cbs.events that failed after all retries` |
| 17 | `transfer.events` | EVENTS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Facts from Fund Transfer: TransferCompleted, TransferFailed, TransferReversed, ReconciliationRequired` |
| 18 | `transfer.events.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from transfer.events that failed after all retries` |
| 19 | `user.events` | EVENTS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Facts from User Service: UserRegistered, EmailVerificationRequested` |
| 20 | `user.events.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from user.events that failed after all retries` |
| 21 | `notification.events` | EVENTS | 3 | 1 | 604800000 (7 days) | 1209600000 (14 days) | `Facts from Notification Service: OtpDeliveryFailed` |
| 22 | `notification.events.DLT` | DEAD_LETTER | 3 | 1 | 1209600000 (14 days) | – | `Messages from notification.events that failed after all retries` |

**22 topics in total: 11 business topics + 11 dead-letter topics.**

### Message types carried by each business topic

The catalog description names the message types. They are the payloads of the commands and facts
that the domain services exchange.

| Topic | Message types (from the catalog description) |
|---|---|
| `account.commands` | `ValidateTransfer`, `ValidateCashAccount` |
| `account.events` | `TransferValidated`, `TransferRejected`, `CashAccountValidated`, `CashAccountRejected`, `AccountOpened` |
| `fee.commands` | `CalculateFee` |
| `fee.events` | `FeeCalculated` |
| `otp.commands` | `IssueOtp` |
| `otp.events` | `OtpIssued` (the code is encrypted) |
| `cbs.commands` | `CreateLedgerAccount`, `CheckBalance`, `Withdraw`, `Deposit`, `Reversal` |
| `cbs.events` | `LedgerAccountCreated`, `BalanceChecked`, `BalanceRejected`, `WithdrawCompleted`, `WithdrawFailed`, `DepositCompleted`, `DepositFailed`, `ReversalCompleted`, `ReversalFailed` |
| `transfer.events` | `TransferCompleted`, `TransferFailed`, `TransferReversed`, `ReconciliationRequired` |
| `user.events` | `UserRegistered`, `EmailVerificationRequested` |
| `notification.events` | `OtpDeliveryFailed` |

The payload fields of these message types are owned by the producing service, not by this one —
this service never reads or writes a message body.

### Why `otp.events` has a shorter retention

The comment in `TopicCatalog.java` explains it:

> `// The retention has to cover the delivery of the OTP, not only its`
> `// lifetime. An OTP is valid for five minutes, but the event has to`
> `// survive a notification service that is down for a while, plus the`
> `// retries. Ten minutes was too short: the event was gone while the`
> `// user was still waiting for the code, and the transfer stayed in`
> `// OTP_PENDING until the saga timeout fired.`

### Replaying a dead-letter topic

The root script `replay-dlt.sh` moves messages from `<topic>.DLT` back to `<topic>` using the
Kafka console tools inside the `banking-kafka` container.

```bash
./replay-dlt.sh user.events          # up to 1000 messages (the default limit)
./replay-dlt.sh user.events 10       # only the last 10 messages
```

It ends with:

```
Check the result: curl -s http://localhost:8090/internal/events/dead-letters
```

## Configuration

### Server and Spring properties

`src/main/resources/application.yaml`:

| Name | Default | Meaning |
|---|---|---|
| `server.port` | `8090` | Public port |
| `spring.application.name` | `kafka-event-service` | The Eureka name this service registers under is `KAFKA-EVENT-SERVICE` |
| `spring.kafka.bootstrap-servers` | `${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}` | The broker this service talks to |
| `spring.kafka.admin.fail-fast` | `true` | The service **refuses to start** if Kafka is unreachable |
| `spring.kafka.admin.modify-topic-configs` | `true` | Manual topic-config changes are reverted on the next start |
| `eureka.client.service-url.defaultZone` | `${EUREKA_URL:http://localhost:8761/eureka}` | The discovery service |
| `eureka.instance.prefer-ip-address` | `true` | Register with the IP address rather than the hostname |
| `management.server.port` | `18090` | Actuator on a private port |
| `management.endpoints.web.exposure.include` | `health,info,prometheus` | Only these three actuator endpoints are exposed |
| `management.metrics.distribution.percentiles-histogram.http.server.requests` | `true` | Enables the latency histogram for HTTP requests |

### Custom `app.*` properties

| Name | Default | Meaning |
|---|---|---|
| `app.kafka.replication-factor` | `1` | Number of replicas for every created topic |

### Environment variables

| Name | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | The Kafka broker to provision topics on |
| `EUREKA_URL` | `http://localhost:8761/eureka` | The discovery service |
| `SERVER_PORT` | `8090` | The public port. `run-all.sh` sets it explicitly to `8090` |

### Environment variables you must set

**None.** Every property has a working default. The one thing that has no default is the *broker
itself* — if nothing is listening on `localhost:9092` the service exits at startup (see
`fail-fast`).

## Behaviour Rules

### The service will not start without a broker

`spring.kafka.admin.fail-fast: true` means the service **exits** when Kafka cannot be reached. The
in-file comment says: *"Stop the service at startup if Kafka cannot be reached, with a clear
error."* This is deliberate — a silently running event bus that created no topics would push the
failure into every publisher.

### The service re-applies the topic settings on every start

`spring.kafka.admin.modify-topic-configs: true` means that if somebody changes a topic's
partition count or `retention.ms` by hand, the next start of this service puts it back to the
catalog values. The in-file comment: *"If someone changes a topic setting by hand, the next start
puts it back."*

### The dead-letter topic always has as many partitions as the original

`addWithDeadLetter` creates the `.DLT` topic with the same `PARTITIONS` constant (3) as the
original — the in-code comment says *"The dead-letter topic needs at least as many partitions as
the original topic."*

### `inSync` is strict

`TopicInfo.inSync` is `true` only when the live partition count **and** the live `retention.ms`
match the catalog **exactly**. A topic with 3 partitions but 6 days of retention is `inSync:
false`.

### Admin calls time out after 10 seconds

`KafkaTopicInspectionService` uses `TIMEOUT_SECONDS = 10` for every `listTopics`,
`describeTopics`, `describeConfigs` and `listOffsets` call.

### The registry is fetched every 30 seconds

This service does **not** set `eureka.client.registry-fetch-interval-seconds`; it uses the Spring
Cloud default of **30 seconds**. A repo-wide grep for `registry-fetch-interval-seconds` returns
exactly **one** hit in the whole platform, and it is in the API gateway
(`api-gateway/src/main/resources/application.yaml:85`), which lowered it to `5`.

### The gateway blocks these endpoints

The routes `/internal/**` are never forwarded. The API gateway's `JwtAuthenticationFilter` answers
**404** for any `/internal` or `/internal/...` path before routing even happens, and the gateway
has a `blocked-internal` route with the URI `no://op`. So the inspection API is reachable **only**
on port 8090 directly.

## Running It

### Prerequisites

- Java 17
- Maven 3.9+ (or the bundled wrapper `./mvnw`)
- **A reachable Kafka broker on `localhost:9092`.** Without it the service exits at startup.

Start the broker with the compose file in this repository:

```bash
cd event-bus-service
docker compose up -d
```

That brings up `apache/kafka:3.9.1` as container `banking-kafka`, published on `127.0.0.1:9092`,
plus `ghcr.io/kafbat/kafka-ui:latest` as `banking-kafka-ui` on `127.0.0.1:8089` (cluster name
`banking`). The broker runs with `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`.

### Build and run

```bash
cd event-bus-service
mvn clean package -DskipTests
java -jar target/kafka-event-service-0.0.1-SNAPSHOT.jar
```

Note the artifact name: the Maven `artifactId` is **`kafka-event-service`**, not
`event-bus-service`, so the jar is `target/kafka-event-service-0.0.1-SNAPSHOT.jar`.

Or as part of the whole platform:

```bash
cd /home/bs01092/Projects/banking-microservices
./run-all.sh
```

`run-all.sh` starts this service second — right after the discovery service — because the topics
must exist before any domain service publishes.

### Environment variables to export

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export EUREKA_URL=http://localhost:8761/eureka
export SERVER_PORT=8090
```

### How to check it is healthy

```bash
# The broker is reachable and every topic exists
curl -s http://localhost:8090/internal/events/health
# {"brokerReachable":true,"allTopicsPresent":true,"missingTopics":[]}

# The live settings of every topic
curl -s http://localhost:8090/internal/events/topics | head -40

# The dead-letter backlog
curl -s http://localhost:8090/internal/events/dead-letters

# The actuator, on the private port
curl -s http://localhost:18090/actuator/health
# {"status":"UP"}

# The actuator is not on the public port
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8090/actuator/health
# 404
```

To check the whole platform:

```bash
cd /home/bs01092/Projects/banking-microservices
./check-all.sh
```

### How to stop it

```bash
# If you started it with ./run-all.sh
cd /home/bs01092/Projects/banking-microservices
./stop-all.sh

# If you started it by hand
kill <pid>
```

### Optional monitoring stack

`event-bus-service/docker-compose.monitoring.yml` runs `danielqsj/kafka-exporter:v1.9.0`,
`prom/prometheus:v3.2.1` on `127.0.0.1:9090` (`--storage.tsdb.retention.time=7d`) and
`grafana/grafana:11.6.0` on `127.0.0.1:3001` (user `admin`, password from
`GRAFANA_ADMIN_PASSWORD`). Prometheus scrapes every service's management port every 15 s.

## API Quick Reference (curl examples)

```bash
# Health of the broker and the topic catalog
curl -s http://localhost:8090/internal/events/health
# {"brokerReachable":true,"allTopicsPresent":true,"missingTopics":[]}

# Every topic with its live partitions and retention
curl -s http://localhost:8090/internal/events/topics

# Just the otp.events entry
curl -s http://localhost:8090/internal/events/topics \
  | python3 -c 'import json,sys; [print(t) for t in json.load(sys.stdin) if t["name"].startswith("otp.")]'

# Dead-letter backlog
curl -s http://localhost:8090/internal/events/dead-letters
# [{"topic":"user.events.DLT","messages":8},{"topic":"otp.events.DLT","messages":1}]

# An unknown path answers 404 with the service's own error body
curl -s -i http://localhost:8090/internal/events/nope
# {"timestamp":"...","status":404,"error":"Not Found","message":"Not found"}

# The gateway refuses to forward /internal/**
curl -s -i http://localhost:8080/internal/events/topics
# {"timestamp":"...","status":404,"error":"Not Found","message":"Not found"}

# The actuator, on the private port only
curl -s http://localhost:18090/actuator/health
curl -s http://localhost:18090/actuator/prometheus | head
```

## Configuration Reference (quick table)

| Knob | Default | Change it when |
|---|---|---|
| `server.port` | `8090` | Port 8090 is taken on your machine |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Your broker lives elsewhere |
| `spring.kafka.admin.fail-fast` | `true` | Never in a normal deployment — it is what stops the platform from running without topics |
| `spring.kafka.admin.modify-topic-configs` | `true` | You manage the topic settings yourself and want this service to leave them alone |
| `app.kafka.replication-factor` | `1` | Your broker is a cluster and you want the topics replicated |
| `eureka.client.service-url.defaultZone` | `http://localhost:8761/eureka` | Your discovery service lives elsewhere |
| `management.server.port` | `18090` | Prometheus already scrapes something on that port |
| `management.endpoints.web.exposure.include` | `health,info,prometheus` | You need more actuator endpoints |

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| The service exits immediately with a Kafka connection error | `spring.kafka.admin.fail-fast: true` and no broker on `localhost:9092` | Start the broker: `docker compose -f event-bus-service/docker-compose.yml up -d` |
| `GET /internal/events/health` answers `503` with `Kafka is not reachable` | The broker is down or unreachable from this process | Check `docker ps` for `banking-kafka` and `KAFKA_BOOTSTRAP_SERVERS` |
| `GET /internal/events/health` answers `503` with a non-empty `missingTopics` | The service was started before the broker, or the topics were deleted | Restart the service — `KafkaAdmin` creates the missing topics on startup |
| `GET /internal/events/topics` shows `inSync: false` | Somebody changed the partition count or `retention.ms` by hand | Restart the service — `modify-topic-configs: true` puts the catalog values back |
| Every event publish fails with `UnknownTopicOrPartitionException` | The broker has auto-create disabled and the topics were never created | Start this service — it creates all 22 topics |
| `run-all.sh` says `Kafka is not reachable on port 9092` and then suggests `docker compose up -d kafka kafka-init` | The hint is wrong: **no `kafka-init` service exists** in either compose file | Run `docker compose -f event-bus-service/docker-compose.yml up -d` instead |
| The gateway answers `404` for `/internal/events/topics` | This is intended — the gateway never forwards `/internal/**` | Call the event bus service directly on port 8090 |
| The actuator answers `404` on port 8090 | The actuator is on the private port 18090 | Use `http://localhost:18090/actuator/health` |
| The service registers in Eureka as `KAFKA-EVENT-SERVICE` and not `EVENT-BUS-SERVICE` | `spring.application.name` is `kafka-event-service` | Expected; the folder name and the artifact name differ from the application name |
| `Port 8090 was already in use` | Another instance is still running | `./stop-all.sh`, or `lsof -i :8090` |

## Notes and Known Limitations

- **The folder name, the artifact name and the application name are all different.**
  Folder `event-bus-service/`, Maven `artifactId` `kafka-event-service`, `<name>` element
  `event-bus-service`, `spring.application.name` `kafka-event-service`, Eureka name
  `KAFKA-EVENT-SERVICE`. The jar is `target/kafka-event-service-0.0.1-SNAPSHOT.jar`.
- **The service has no dashboard and no publisher.** It creates the namespace and nothing else. If
  you want to see the traffic, use the Kafka UI on `127.0.0.1:8089`.
- **The dead-letter topics are created by this service, but the dead-letter *routing* is not.** The
  `.DLT` suffix matches what Spring Kafka's dead-letter handling produces, but the listener
  containers that park a failed message live in the domain services.
- **`otp.events` is the only topic with a retention shorter than a day** (2 hours, and 4 hours for
  its `.DLT`). Everything else is 7 days, and every `.DLT` is 14 days.
- **The replication factor is 1** (`app.kafka.replication-factor: 1`), which is right for a single
  broker and wrong for a real cluster. Raise it before you run this against more than one broker —
  note that the topic settings are re-applied on every start, so a change in `app.kafka.replication-factor`
  also rewrites the existing topics.
- **The registry is fetched every 30 s**, the Spring Cloud default. Only the API gateway shortens
  it, to 5 s.
- **The `micrometer-registry-prometheus` dependency carries a comment** that explains why it is
  there: *"Without this registry there is no /actuator/prometheus endpoint, and Prometheus scrapes
  this service every 15 seconds."*
- **The test is a plain `contextLoads()`** in
  `src/test/java/com/banking/eventbus/EventBusServiceApplicationTests.java`.
- **Maven coordinates**: `com.banking:kafka-event-service:0.0.1-SNAPSHOT`. Dependencies:
  `spring-boot-starter-web`, `spring-boot-starter-actuator`, `micrometer-registry-prometheus`,
  `spring-kafka`, `spring-cloud-starter-netflix-eureka-client`, `spring-boot-starter-test`.
  Spring Boot 3.5.5, Spring Cloud BOM 2025.0.0, Java 17.
