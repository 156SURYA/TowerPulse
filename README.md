# 📡 TowerPulse — Real-Time Telecom Congestion Analytics

A complete, multi-layer data engineering project that processes Call Detail Records (CDRs) through four progressive layers — MapReduce, Hive, Spark Batch, and Spark Structured Streaming — to detect and alert on cell tower congestion in real time.

Built and run on a local single-node Hadoop cluster (HDFS datanode + namenode) on WSL Ubuntu.

---

## 🏗️ Architecture

```
CDR CSV Data (10,000 records)
        │
        ▼
┌─────────────────────────────┐
│  Layer 1: MapReduce (Java)  │  Tower call count via Hadoop MapReduce
│  TowerMapper + TowerReducer │  Input: HDFS /user/telecom/raw/batch
└────────────┬────────────────┘  Output: HDFS /user/telecom/processed/mr_output
             │
        ▼
┌─────────────────────────────┐
│  Layer 2: Hive              │  SQL analytics on CDR data
│  telecom database           │  External Parquet tables over HDFS
└────────────┬────────────────┘  Views: daily_congestion_summary, sla_breaches
             │
        ▼
┌─────────────────────────────┐
│  Layer 3: Spark Batch       │  DataFrame API + Spark SQL analytics
│  CDR partitioned by date    │  Tower traffic, hourly patterns, call types
└────────────┬────────────────┘  Output: Parquet to HDFS
             │
        ▼
┌──────────────────────────────────────────────────┐
│  Layer 4: Spark Structured Streaming             │
│  Kafka(telecom_cdr)                              │
│    → Watermark (10 min) + Window Aggregation     │
│    → Congestion Classification                   │
│    → Parquet (HDFS) + Kafka(telecom_alerts)      │
└──────────────────────────────────────────────────┘
             │
        ▼
┌─────────────────────────────┐
│  Layer 5: Hive on Stream    │  External tables over streaming Parquet output
│  tower_congestion_rt        │  Queries: congestion trends, SLA breaches
│  tower_congestion_batch     │
│  congested_towers           │
└─────────────────────────────┘
```

---

## 🎯 Problem Statement

Telecom operators need to detect tower congestion within minutes, not hours. Batch reports are too slow. This project addresses the full data lifecycle:

- Historical batch analytics via MapReduce and Spark
- Real-time detection via Kafka + Spark Structured Streaming
- Persistent storage in Hive-compatible Parquet for downstream reporting
- Immediate alerting for CRITICAL congestion windows

---

## 📊 CDR Schema

| Field     | Type      | Description             |
|-----------|-----------|-------------------------|
| call_id   | String    | Unique call identifier  |
| caller    | Long      | Caller phone number     |
| receiver  | Long      | Receiver phone number   |
| call_type | String    | VOICE / SMS / DATA      |
| duration  | Integer   | Call duration (seconds) |
| call_date | Timestamp | Event time of the call  |
| tower_id  | String    | Cell tower identifier   |

---

## 🔴 Congestion Classification

| Total calls (5-min window, per tower) | Status   |
|---------------------------------------|----------|
| < 3                                   | NORMAL   |
| 3 – 4                                 | HIGH     |
| ≥ 5                                   | CRITICAL |

Only `CRITICAL` windows are published as Kafka alerts.

---

## 📁 Repo Structure

```
TowerPulse/
│
├── mapreduce/
│   ├── Driver.java                  # Hadoop job driver
│   ├── TowerMapper.java             # Emits (tower_id, 1) per CDR record
│   ├── TowerReducer.java            # Aggregates total calls per tower
│   └── MapReduce_Explanation.txt    # Job design notes
│
├── hive/
│   └── hive_design.sql              # Hive database, external tables, views
│
├── spark/
│   ├── TowerPulse.scala             # Spark Structured Streaming job
│   ├── HiveAnalytics.scala          # Batch Hive reporting on stream output
│   └── CDRProducer.scala            # Kafka CDR producer for testing
│
├── scripts/
│   └── hive-java8.sh                # Hive environment setup (Java 8)
│
├── data/
│   ├── generate_cdr.py              # Generates 20 batches × 500 CDR records
│   └── sample_cdr.csv               # Sample data covering NORMAL/HIGH/CRITICAL
│
└── README.md
```

---

## ⚙️ Layer 1 — MapReduce (Java)

Processes CDR CSV files from HDFS and counts total calls per tower.

```bash
# Compile
javac -classpath $(hadoop classpath) *.java
jar cf tower-count.jar *.class

# Run
hadoop jar tower-count.jar Driver \
  /user/telecom/raw/batch \
  /user/telecom/processed/mr_output
```

---

## ⚙️ Layer 2 — Hive Analytics

```bash
# Set Java 8 environment
source scripts/hive-java8.sh

# Run Hive setup
hive -f hive/hive_design.sql
```

Key tables and views created:
- `telecom.cdr` — raw CDR data
- `telecom.tower_congestion_rt` — external table over streaming Parquet output
- `telecom.tower_congestion_batch` — batch congestion output
- `telecom.congested_towers` — curated CRITICAL/HIGH events
- `telecom.daily_congestion_summary` — view: congestion windows per tower per day
- `telecom.sla_breaches` — view: HIGH and CRITICAL events

---

## ⚙️ Layer 3 — Spark Batch

```bash
spark-shell \
  --conf spark.sql.warehouse.dir=/user/hive/warehouse \
  --conf spark.sql.catalogImplementation=hive
```

Reads CDR from Hive, runs tower-wise traffic analysis, hourly patterns, call type breakdown. Writes partitioned Parquet output to HDFS.

---

## ⚙️ Layer 4 — Spark Structured Streaming

```bash
# Start Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh config/server.properties

# Create topics
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic telecom_cdr --partitions 1 --replication-factor 1

$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic telecom_alerts --partitions 1 --replication-factor 1

# Start streaming job
spark-shell \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.warehouse.dir=/user/hive/warehouse \
  --conf spark.sql.catalogImplementation=hive \
  -i spark/TowerPulse.scala

# Produce CDR data
spark-shell -i spark/CDRProducer.scala

# Watch alerts
$KAFKA_HOME/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic telecom_alerts \
  --from-beginning
```

---

## 🚨 Sample Alert Output

```json
{
  "tower_id": "TOWER_M",
  "total_calls": 5,
  "status": "CRITICAL",
  "window_start": "2026-01-27T12:45:00.000Z",
  "window_end": "2026-01-27T12:50:00.000Z"
}
```

---

## 🧪 Generating Test Data

```bash
python data/generate_cdr.py
# Produces 20 batches × 500 records = 10,000 CDR records
# One batch every 10 seconds to simulate live traffic
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Generation | Python |
| Distributed Storage | HDFS (local single-node cluster) |
| Batch Processing | Hadoop MapReduce (Java) |
| SQL Analytics | Apache Hive |
| Batch Analytics | Apache Spark (Scala) |
| Stream Ingestion | Apache Kafka |
| Stream Processing | Spark Structured Streaming (Scala) |
| Storage Format | Parquet |

---

## 🧠 Key Concepts Applied

- Hadoop MapReduce — mapper/reducer pattern, JAR packaging, HDFS I/O
- Hive — external tables, partitioned tables, views, Parquet storage
- Spark DataFrame API — schema inference, column renaming, partitioned writes
- Spark SQL — `createOrReplaceTempView`, HiveContext integration
- Spark Structured Streaming — event-time processing, watermarking, tumbling windows
- Kafka — producer/consumer workflow, topic management, exactly-once semantics
- Dual sink architecture — Parquet persistence + Kafka alerting from single stream

---

## 👤 Author

G N Surya Jain
