# 📡 Real-Time Telecom Congestion Analytics & Alerting System

## 📌 Overview

This project implements a **real-time data engineering pipeline** to monitor telecom call traffic and detect **cell tower congestion** using **Apache Kafka** and **Apache Spark Structured Streaming**.

The system continuously ingests Call Detail Records (CDRs), performs **event-time windowed aggregation**, classifies congestion levels, and generates **real-time alerts** for critical tower overloads.

---

## 🎯 Problem Statement

Telecom operators must monitor call traffic in real time to:

* Detect sudden spikes in call volume per tower
* Identify congestion early
* Trigger alerts to avoid service degradation

Batch processing is insufficient for such time-sensitive scenarios. This project addresses the problem using a **streaming-first architecture**.

---

## 🏗️ Architecture

```
Kafka (telecom_cdr topic)
        ↓
Spark Structured Streaming
  - JSON parsing
  - Event-time processing
  - Watermarking
  - Windowed aggregation
        ↓
Congestion Classification
  - NORMAL
  - HIGH
  - CRITICAL
        ↓
Kafka (telecom_alerts topic)
```

---

## 🛠️ Technologies Used

* **Apache Kafka** – real-time data ingestion and alert publishing
* **Apache Spark Structured Streaming (Scala)** – stream processing
* **ZooKeeper** – Kafka coordination
* **Linux / WSL** – execution environment

---

## 📊 Input Data (CDR Schema)

| Field     | Type      | Description             |
| --------- | --------- | ----------------------- |
| call_id   | String    | Unique call identifier  |
| caller    | Long      | Caller phone number     |
| receiver  | Long      | Receiver phone number   |
| call_type | String    | Voice / Data            |
| duration  | Integer   | Call duration (seconds) |
| call_date | Timestamp | Event time of call      |
| tower_id  | String    | Cell tower identifier   |

---

## ⚙️ Processing Logic

### 🔹 Event-Time Processing

* Uses `call_date` as event time
* Handles late-arriving data using watermarking

```scala
.withWatermark("call_date", "10 minutes")
```

---

### 🔹 Windowed Aggregation

* 5-minute tumbling windows per tower

```scala
groupBy(
  window(col("call_date"), "5 minutes"),
  col("tower_id")
)
```

---

### 🔹 Congestion Classification

| Total Calls (per window) | Status   |
| ------------------------ | -------- |
| < 3                      | NORMAL   |
| 3 – 4                    | HIGH     |
| ≥ 5                      | CRITICAL |

Only **CRITICAL** events are published as alerts.

---

## 🚨 Output (Alert Message Example)

```json
{
  "tower_id": "T1",
  "total_calls": 5,
  "status": "CRITICAL",
  "window_start": "2026-01-30T11:00:00",
  "window_end": "2026-01-30T11:05:00"
}
```

---

## ▶️ How to Run the Project

### 1️⃣ Start ZooKeeper

```bash
zookeeper-server-start.sh config/zookeeper.properties
```

### 2️⃣ Start Kafka Broker

```bash
kafka-server-start.sh config/server.properties
```

### 3️⃣ Create Kafka Topics

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create \
--topic telecom_cdr --partitions 1 --replication-factor 1
```

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create \
--topic telecom_alerts --partitions 1 --replication-factor 1
```

### 4️⃣ Start Spark Shell

```bash
spark-shell \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
```

### 5️⃣ Run Streaming Application

* Define schema
* Read from `telecom_cdr`
* Apply windowed aggregation & alert logic
* Write alerts to `telecom_alerts`

---

## 🧪 Testing the Pipeline

### Produce Test Data

```bash
kafka-console-producer.sh \
--bootstrap-server localhost:9092 \
--topic telecom_cdr
```

Paste sample JSON records to simulate call traffic.

### Consume Alerts

```bash
kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic telecom_alerts \
--from-beginning
```

---

## 🧠 Key Learnings & Concepts

* Kafka producer/consumer workflow
* Spark Structured Streaming internals
* Event-time vs processing-time
* Watermarking and late data handling
* Windowed aggregations
* Real-time alerting pipelines
* Debugging distributed systems

---

## 🚀 Future Enhancements

* Persist aggregated data to **HDFS / Parquet**
* Create **Hive external tables** for analytics
* Add **dashboard (Grafana / Streamlit)**
* Dockerize Kafka & Spark
* Deploy on cloud (AWS / GCP)

---

## 📝 Resume Summary

> Built a real-time telecom congestion monitoring pipeline using Apache Kafka and Spark Structured Streaming with event-time windowing, watermarking, and real-time alert generation.

---

## 👤 Author

**G N Surya Jain**

----
