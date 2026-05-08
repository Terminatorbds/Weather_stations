# Weather Stations Monitoring System

A distributed stream-processing pipeline that simulates 10 IoT weather stations emitting live readings into Apache Kafka, detects rain events in real time, and persists data into PostgreSQL via batch inserts. The entire system runs on Kubernetes.

> **Course:** Net-Centric Computing — Final Project, Fall 2025–2026
> **University:** AAST
> **Stack:** Java 17 · Apache Kafka (KRaft) · Kafka Streams · PostgreSQL 16 · Docker · Kubernetes (Minikube) · Maven

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Repository Structure](#repository-structure)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [Detailed Setup](#detailed-setup)
6. [Running the System](#running-the-system)
7. [Observability](#observability)
8. [Analytical SQL Queries](#analytical-sql-queries)
9. [Design Decisions](#design-decisions)
10. [Troubleshooting](#troubleshooting)
11. [Tearing Down](#tearing-down)
12. [Theory Connection](#theory-connection)

---

## System Architecture

```
                              ┌─────────────────────────────────┐
                              │   Apache Kafka (KRaft mode)     │
   ┌───────────────────┐      │                                 │
   │ Weather Station 1 │─────▶│  Topic: weather-readings        │
   │ Weather Station 2 │─────▶│  (1 partition, all stations)    │
   │       ...         │─────▶│                                 │
   │ Weather Station 10│─────▶│  Topic: rain-alerts             │◀─┐
   └───────────────────┘      └────────────┬────────────────────┘  │
   (StatefulSet, 10 replicas)              │                       │
   1 msg/sec each, 10% drop                │                       │
                                           │                       │
                              ┌────────────┴────────┐    ┌─────────┴────────┐
                              │  Central Station    │    │  Rain Processor  │
                              │  (Deployment, 1)    │    │ (Kafka Streams)  │
                              │                     │    │                  │
                              │  Buffer 5000 →      │    │  filter:         │
                              │  Batch INSERT       │    │  humidity > 70%  │
                              └──────────┬──────────┘    └──────────────────┘
                                         │
                                         ▼
                              ┌──────────────────────┐
                              │  PostgreSQL 16       │
                              │  weather_readings    │
                              │  + 1 GiB PVC         │
                              └──────────────────────┘
```

### Components

| Component | Role | Replicas |
|---|---|---|
| **Weather Station** | Emits a JSON reading every 1 second; weighted battery status (30% low / 40% medium / 30% high); 10% intentional drop rate | 10 (StatefulSet) |
| **Kafka** | Single-broker KRaft cluster (no Zookeeper) | 1 |
| **Rain Processor** | Kafka Streams app: reads `weather-readings`, filters humidity > 70, writes to `rain-alerts` | 1 |
| **Central Station** | Kafka consumer; buffers messages and batch-inserts 5000 rows at a time into PostgreSQL | 1 |
| **PostgreSQL** | Persistent storage backed by a 1 GiB PersistentVolumeClaim | 1 |
| **Kafka UI** | Web dashboard for inspecting topics, messages, and consumer groups | 1 |

### Message schema

```json
{
  "station_id": 1,
  "s_no": 42,
  "battery_status": "medium",
  "status_timestamp": 1715184738,
  "weather": {
    "humidity": 56,
    "temperature": 78,
    "wind_speed": 22
  }
}
```

---

## Repository Structure

```
Weather_stations/
├── pom.xml                              # Maven parent (multi-module)
├── docker-compose.yml                   # Local development stack (optional)
├── README.md                            # This file
├── RUN.md                               # Quick run-commands reference
│
├── weather-station/                     # Producer service
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/
│       ├── java/com/weather/station/
│       │   ├── WeatherStation.java      # main(); Kafka producer loop
│       │   ├── WeatherReading.java      # data record
│       │   └── ReadingGenerator.java    # weighted random generation
│       └── resources/logback.xml
│
├── kafka-processor/                     # Rain detection (Kafka Streams)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/
│       ├── java/com/weather/processor/
│       │   └── RainProcessor.java       # filter humidity > 70 → rain-alerts
│       └── resources/logback.xml
│
├── central-station/                     # Consumer + DB writer
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/
│       ├── java/com/weather/central/
│       │   ├── CentralStation.java      # main(); consume + buffer + flush
│       │   ├── WeatherReadingDao.java   # JDBC batch INSERT (size 5000)
│       │   └── WeatherReadingDto.java   # data record
│       └── resources/logback.xml
│
├── k8s/                                 # Kubernetes manifests
│   ├── configmap.yaml                   # Kafka URL, DB URL (non-secret)
│   ├── secret.yaml                      # DB credentials (base64)
│   ├── postgres-pvc.yaml                # 1 GiB PersistentVolumeClaim
│   ├── postgres-deployment.yaml         # Postgres + ClusterIP service
│   ├── kafka-deployment.yaml            # Kafka (KRaft) + ClusterIP service
│   ├── kafka-ui-deployment.yaml         # Web UI for Kafka inspection
│   ├── weather-station-deployment.yaml  # StatefulSet of 10 stations
│   ├── kafka-processor-deployment.yaml  # Rain processor deployment
│   └── central-station-deployment.yaml  # Central station deployment
│
└── sql/
    ├── schema.sql                       # weather_readings table + indexes
    ├── query1_battery.sql               # Battery distribution per station
    └── query2_drops.sql                 # Estimated dropped messages
```

---

## Prerequisites

Tools required (versions used during development shown):

| Tool | Min Version | Used |
|---|---|---|
| **JDK** | 17 | Eclipse Temurin 17.0.19 |
| **Maven** | 3.8 | 3.9.15 |
| **Docker** | 24 | Docker Desktop 28.3.2 |
| **kubectl** | 1.28 | 1.32.2 |
| **Minikube** | 1.30 | 1.38.1 |

Resource requirements: Minikube needs at least **4 CPU / 4 GiB RAM**.

---

## Quick Start

For the impatient — assumes you've already built the images at least once.

```powershell
# 1. Start Minikube
minikube start --memory=4096 --cpus=4 --driver=docker

# 2. Load the three custom images into Minikube
minikube image load weather-station:1.0
minikube image load kafka-processor:1.0
minikube image load central-station:1.0

# 3. Apply all Kubernetes manifests
kubectl apply -f k8s/

# 4. Wait for all pods to be Running (Ctrl+C the watch when ready)
kubectl get pods -w

# 5. Apply the database schema
$pgpod = (kubectl get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}')
Get-Content sql\schema.sql | kubectl exec -i $pgpod -- psql -U weather_user -d weather_db

# 6. Confirm data is flowing
kubectl logs -f deployment/central-station
```

---

## Detailed Setup

### 1. Install the toolchain (Windows)

```powershell
# JDK 17
winget install --id EclipseAdoptium.Temurin.17.JDK

# Maven (manual install from https://maven.apache.org/download.cgi if winget fails)

# Minikube
winget install --id Kubernetes.minikube
```

After install, verify in a *fresh* PowerShell (env vars only refresh in new shells):

```powershell
java -version       # Should be 17.x
javac -version      # Should be 17.x
mvn -version
minikube version
docker --version
kubectl version --client
```

### 2. Build the JAR artifacts

From the project root:

```powershell
mvn clean package
```

This builds all three modules and produces:
- `weather-station/target/weather-station.jar`
- `kafka-processor/target/kafka-processor.jar`
- `central-station/target/central-station.jar`

Each is a "fat JAR" (uber-jar) with all dependencies bundled, runnable via `java -jar`.

### 3. Build the Docker images

```powershell
docker build -f weather-station/Dockerfile -t weather-station:1.0 .
docker build -f kafka-processor/Dockerfile -t kafka-processor:1.0 .
docker build -f central-station/Dockerfile -t central-station:1.0 .
```

Each Dockerfile uses a **multi-stage build**: a heavy `maven:3.9-eclipse-temurin-17` image compiles the code, then only the final JAR is copied into a slim `eclipse-temurin:17-jre` runtime image (~320 MB final size).

### 4. Load images into Minikube

Minikube runs its own Docker daemon, separate from the host. Custom images must be explicitly loaded:

```powershell
minikube image load weather-station:1.0
minikube image load kafka-processor:1.0
minikube image load central-station:1.0
```

Each transfers ~300 MB; allow 30–60 seconds per image. Each manifest sets `imagePullPolicy: Never` so K8s won't try to pull from a registry.

### 5. Apply the manifests

```powershell
kubectl apply -f k8s/
```

This creates **14 objects**:
- 1 ConfigMap, 1 Secret, 1 PVC
- 5 Deployments (postgres, kafka, kafka-ui, kafka-processor, central-station)
- 1 StatefulSet (weather-station, replicas=10)
- 5 Services (one per network-accessible component, plus a headless Service for the StatefulSet)

Result: **14 pods** running (1 each for postgres, kafka, kafka-ui, processor, central — 5; plus 10 weather stations from the StatefulSet — 15 total minus the headless service which contributes no pod = 14 pods).

### 6. Apply the schema

PostgreSQL starts with an empty database. Run the schema once after the postgres pod is up:

```powershell
$pgpod = (kubectl get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}')
Get-Content sql\schema.sql | kubectl exec -i $pgpod -- psql -U weather_user -d weather_db
```

---

## Running the System

Once `kubectl get pods` shows everything `Running`, the system is live and producing data immediately. Each weather station emits ~0.9 messages/second (1 per second × 90% non-drop rate). With 10 stations, total throughput is ~9 messages/second.

The central station accumulates messages in memory and flushes to PostgreSQL when **either**:
- The buffer reaches **5000 records** (occurs every ~9 minutes at full traffic), **or**
- **10 minutes** have elapsed since the buffer's first message (safety valve to prevent indefinite buffering during low-traffic periods).

### Tailing logs

```powershell
kubectl logs -f deployment/central-station        # batch insert events
kubectl logs -f deployment/kafka-processor        # rain alerts
kubectl logs -f weather-station-0                  # one station's output
```

---

## Observability

The project ships with two web UIs and the K8s Dashboard, all accessible via port-forwarding.

### Kafka UI (topic & message inspection)

```powershell
kubectl port-forward svc/kafka-ui-svc 8080:8080
```

Open http://localhost:8080. Provides:
- Live topic list (`weather-readings`, `rain-alerts`, internal Streams topics)
- Message count, throughput, retention info
- Consumer group lag (watch `central-station` lag climb to ~5000 then snap to 0 every batch flush)
- Click into any message to see its JSON content

### Kubernetes Dashboard

```powershell
minikube dashboard
```

Auto-opens the browser. Provides:
- All pods/services/deployments with live status
- Per-pod logs viewer
- Cluster events (image pulls, pod creation, restarts)
- One-click scaling and restart actions

### CLI quick checks

```powershell
kubectl get pods                                  # snapshot
kubectl get pods -w                                # watch mode
kubectl describe pod <name>                        # detailed status
kubectl logs <name> --previous                     # logs from a crashed pod
```

---

## Analytical SQL Queries

These are the two queries the project specification requires. Run after the system has been live for at least 5 minutes (ideally 25+ minutes for a meaningful sample of full-size batches).

### Setup

```powershell
$pgpod = (kubectl get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}')
```

### Query 1 — Battery distribution per station

```powershell
Get-Content sql\query1_battery.sql | kubectl exec -i $pgpod -- psql -U weather_user -d weather_db
```

Expected: roughly 30% low / 40% medium / 30% high per station, matching the producer's weighted random distribution. Sample-size variance is normal — exact percentages converge as more data accumulates.

### Query 2 — Estimated dropped messages

```powershell
Get-Content sql\query2_drops.sql | kubectl exec -i $pgpod -- psql -U weather_user -d weather_db
```

**Important interpretation note:** Per the spec, `s_no` is incremented **only on actual sends, not on drops**. Therefore:
- `MAX(sequence_number)` = number of messages this station *successfully sent* to Kafka
- `COUNT(*)` = number of messages the database has *received and stored*
- Their difference reflects **post-Kafka** message loss (network issues, consumer-side crashes), not the deliberate 10% producer drop. In a healthy steady-state run, this delta is near zero.

The 10% drop rate is implicit and not measurable from this query alone — it would require comparing a separate "attempted send" counter, which the spec does not require us to maintain.

---

## Design Decisions

These choices are worth highlighting in the technical report:

### KRaft mode over Zookeeper
We chose Confluent's `cp-kafka:7.7.1` image running in KRaft mode rather than the classical Bitnami Kafka + Zookeeper combination. KRaft replaced Zookeeper as Kafka's coordination protocol starting in version 3.x, eliminating one container, one network port, and one source of operational complexity. The Bitnami images were also relocated to a paid namespace shortly before this project, making Confluent the more sustainable choice.

### StatefulSet over Deployment for stations
Weather stations could technically run as a Deployment (replicas don't share state), but we chose a StatefulSet to obtain **stable, ordinal pod names** (`weather-station-0` through `weather-station-9`). The trailing ordinal is parsed at runtime to derive the `Long station_id` field cleanly (1 through 10). This gives readable database output and matches the spec's intent that station IDs be small integers.

### Batch size 5000, with a 10-minute safety valve
The spec mandates 5000-record batches. At ~9 messages/second total ingest, a strict size-only flush would naturally fill in ~9 minutes. We added a 10-minute time-based safety valve — *just slightly longer* than the natural fill time — so partial batches eventually persist if traffic ever drops below the design rate. In practice, the time-based trigger almost never fires, and batches consistently land at exactly 5000 rows.

### "DB first, commit second" consumer pattern
The central station uses **manual offset commits** (`enable.auto.commit=false`). After a successful batch insert, we call `consumer.commitSync()` to advance the consumer group's position in Kafka. If the DB insert fails, we don't commit — Kafka retains the messages and replays them on the next poll. This implements **at-least-once delivery semantics** end-to-end. (At-most-once would commit first; exactly-once requires Kafka transactions, out of scope here.)

### `acks=all` and `linger.ms=10` on the producer
`acks=all` ensures the broker confirms full replication before the producer treats a send as successful (highest durability). `linger.ms=10` lets the producer batch up to 10 ms of records into a single request, trading 10 ms of latency for significantly higher throughput. Both are production-grade defaults.

### Multi-stage Docker builds
Each service's Dockerfile uses a heavy build stage (`maven:3.9-eclipse-temurin-17`, ~700 MB) and a slim runtime stage (`eclipse-temurin:17-jre`, ~200 MB). Only the final JAR crosses the stage boundary. This reduces final image size by ~75% and shrinks the attack surface (no compiler in production).

### Tagged images, never `:latest`
All custom images are tagged `:1.0` rather than the default `:latest`. Pinned tags make deployments reproducible — six months from now, "weather-station:1.0" still means the same code. `:latest` is a moving target and a common production footgun.

### ConfigMap + Secret for runtime configuration
No service has Kafka URLs, DB URLs, or credentials hardcoded. Non-sensitive config lives in `weather-config` (ConfigMap), credentials in `weather-secret` (Secret, base64-encoded). Pods receive them as environment variables. This is the standard 12-factor pattern: identical images run in any environment by changing only the config.

> **A note on Secret encryption:** Kubernetes Secrets are *encoded* (base64), not encrypted at rest by default. For this project they keep credentials out of source code and pod logs. In production, additional measures like sealed-secrets, external secret managers (Vault, AWS Secrets Manager), or enabling encryption-at-rest in etcd would be required.

---

## Troubleshooting

### Pods stuck in `ImagePullBackOff`
The image isn't loaded into Minikube. Run:
```powershell
minikube image ls | Select-String "weather-station|kafka-processor|central-station"
minikube image load <missing-image>:1.0
```

### Pods in `CrashLoopBackOff` after a code change
Minikube cached the old image under the same tag. Force-replace:
```powershell
minikube ssh -- docker rmi -f <image>:1.0
minikube image load <image>:1.0
kubectl rollout restart <deployment-or-statefulset>/<name>
```

### Maven `clean` fails with "file in use" on Windows
A leftover `java.exe` is holding a JAR file open. Kill all Java processes:
```powershell
Stop-Process -Name java -Force -ErrorAction SilentlyContinue
mvn clean package
```

### Central station: `relation "weather_readings" does not exist`
The schema wasn't applied to the K8s Postgres pod. Apply it:
```powershell
$pgpod = (kubectl get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}')
Get-Content sql\schema.sql | kubectl exec -i $pgpod -- psql -U weather_user -d weather_db
kubectl rollout restart deployment/central-station
```

### Rain processor: `MissingSourceTopicException`
The `weather-readings` topic was deleted while the processor was running. Kafka Streams treats missing source topics as fatal. Wait for stations to recreate the topic (they will on their next send), then restart the processor:
```powershell
kubectl rollout restart deployment/kafka-processor
```

### Minikube won't start / out of memory
Increase resources:
```powershell
minikube stop
minikube delete
minikube start --memory=6144 --cpus=4 --driver=docker
```

### `kubectl` connection refused
Minikube is stopped. Start it:
```powershell
minikube start
kubectl config use-context minikube
```

---

## Tearing Down

### Pause everything (preserves all state — fast restart later)
```powershell
minikube stop
```

### Delete K8s objects but keep Minikube
```powershell
kubectl delete -f k8s/
```

### Full reset (deletes Minikube and all data)
```powershell
minikube delete
```

### Clean slate for re-demo (keep cluster, wipe data)
```powershell
# Stop central station to avoid mid-truncate inserts
kubectl scale deployment/central-station --replicas=0

# Truncate the DB
$pgpod = (kubectl get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}')
kubectl exec -i $pgpod -- psql -U weather_user -d weather_db -c "TRUNCATE weather_readings RESTART IDENTITY;"

# Skip Kafka backlog (so central station starts from "now" instead of replaying)
$kpod = (kubectl get pods -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $kpod -- kafka-consumer-groups --bootstrap-server localhost:9092 `
  --group central-station --topic weather-readings --reset-offsets --to-latest --execute

# Bring central station back online
kubectl scale deployment/central-station --replicas=1
```

---

## Theory Connection

Brief mapping to *Distributed Systems* (van Steen & Tanenbaum, 3rd Ed.) for the report:

**Chapter 4 — Communication:** Kafka implements **persistent asynchronous communication**. Producers don't wait for consumers; messages are durably stored on the broker and survive independently of either party. Kafka offsets are how the system implements **at-least-once delivery semantics**: the consumer's commit point advances only after successful processing, so a crash before commit results in replay rather than loss.

**Chapter 6 — Coordination:** KRaft replaces Zookeeper as Kafka's distributed coordination layer, using the Raft consensus protocol. The controller quorum elects a leader, replicates metadata, and tolerates `(n-1)/2` controller failures. In our single-broker setup the quorum is degenerate (1-of-1), but the same protocol scales horizontally to multi-broker production clusters. Kubernetes itself uses a similar consensus mechanism (etcd, also Raft-based) to coordinate cluster state.

**Chapter 8 — Fault tolerance:** The "DB first, commit second" pattern in the central station is a classic example of **idempotent at-least-once processing** to recover from consumer failures. The 10% producer drop rate simulates **intermittent partial failures**. The PVC-backed Postgres pod survives pod restarts (data is on the volume, not the pod's filesystem). Kubernetes' restart policies provide **automatic crash recovery** at the workload layer. Together these three mechanisms — Kafka's replay semantics, persistent volumes, and K8s pod restarts — give the system end-to-end fault tolerance without any custom recovery code in the services themselves.

---

## License & Acknowledgments

Educational project for AAST Net-Centric Computing, Fall 2025–2026.

Built on top of Apache Kafka, PostgreSQL, Confluent Platform images, Provectus Kafka UI, and the Eclipse Temurin JDK — all open source.
