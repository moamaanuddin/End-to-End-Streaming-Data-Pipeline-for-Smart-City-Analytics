<div align="center">

# 🚀 SparkCity

### Real-Time Smart City Data Engineering Platform

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black)
![PySpark](https://img.shields.io/badge/PySpark-Structured%20Streaming-orange)
![AWS S3](https://img.shields.io/badge/AWS-S3-yellow)
![AWS Glue](https://img.shields.io/badge/AWS-Glue-red)
![Amazon Athena](https://img.shields.io/badge/Amazon-Athena-purple)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![Data Engineering](https://img.shields.io/badge/Data-Engineering-green)

**🏙️ End-to-End Smart City Data Lake built using Kafka, PySpark, AWS S3, Glue & Athena**

</div>

---

# 📖 Project Overview

SparkCity is a cloud-native real-time data engineering platform designed to simulate and process smart city events at scale.

The system generates synthetic urban mobility and environmental data streams and ingests them into Apache Kafka. PySpark Structured Streaming consumes these streams in real time and stores them as Parquet datasets in an AWS S3 Data Lake.

AWS Glue Crawlers automatically discover schemas and create metadata tables in the AWS Glue Data Catalog. Amazon Athena enables serverless SQL analytics directly on top of the data lake.

---

# 🏗️ Solution Architecture

```text
                    Smart City Simulator
                            │
                            ▼
                    Apache Kafka Topics
                            │
                            ▼
              PySpark Structured Streaming
                            │
                            ▼
                   AWS S3 Data Lake
                            │
                            ▼
                    AWS Glue Crawler
                            │
                            ▼
                  AWS Glue Data Catalog
                            │
                            ▼
                     Amazon Athena
                            │
                            ▼
                 Analytics & Insights
```

---

# ⚡ Technologies Used

| Layer               | Technology                   |
| ------------------- | ---------------------------- |
| Programming         | Python                       |
| Event Streaming     | Apache Kafka                 |
| Processing Engine   | Apache Spark                 |
| Stream Processing   | PySpark Structured Streaming |
| Storage             | AWS S3                       |
| Metadata Management | AWS Glue Data Catalog        |
| Schema Discovery    | AWS Glue Crawler             |
| Query Engine        | Amazon Athena                |
| File Format         | Apache Parquet               |
| Containerization    | Docker                       |
| Infrastructure      | Docker Compose               |

---

# 🌊 Data Streams

### 🚗 Vehicle Telemetry

* Vehicle Location
* Speed
* Vehicle Metadata
* Fuel Information

### 📍 GPS Tracking

* Vehicle Coordinates
* Travel Direction
* Speed Monitoring

### 📷 Traffic Camera Events

* Camera Metadata
* Snapshot Information

### 🌦️ Weather Monitoring

* Temperature
* Humidity
* Wind Speed
* Air Quality Index

### 🚨 Emergency Incidents

* Accidents
* Medical Events
* Fire Incidents
* Incident Status

---

# 📂 Data Lake Structure

```text
s3://spark-city-streaming-data-buck/

data/
│
├── vehicle_data/
├── gps_data/
├── traffic_camera_data/
├── weather_data/
└── emergency_incident_data/

checkpoint/
│
├── vehicle_data/
├── gps_data/
├── traffic_camera_data/
├── weather_data/
└── emergency_incident_data/
```

## 📸 Project Screenshots

🔗 [AWS S3 Data Lake](docs/images/s3-data-lake.png)

🔗 [AWS Glue Crawler](docs/images/glue-crawler.png)

🔗 [AWS Glue Data Catalog](docs/images/glue-catalog.png)

🔗 [Amazon Athena Tables](docs/images/athena-tables.png)

# 🔄 Pipeline Workflow

1. Python simulator generates smart city events.
2. Events are published into Kafka topics.
3. PySpark Structured Streaming consumes Kafka streams.
4. Data is validated against predefined schemas.
5. Processed data is written to AWS S3 in Parquet format.
6. AWS Glue Crawler scans S3 locations.
7. AWS Glue Catalog creates metadata tables.
8. Amazon Athena queries data directly from the Data Lake.

---

# 🚀 Key Features

✅ Real-Time Event Streaming

✅ Kafka Multi-Topic Architecture

✅ PySpark Structured Streaming

✅ Watermark-Based Stream Processing

✅ AWS S3 Data Lake

✅ Apache Parquet Storage

✅ AWS Glue Crawler Integration

✅ AWS Glue Data Catalog

✅ Amazon Athena Query Engine

✅ Dockerized Deployment

✅ Fault-Tolerant Checkpointing

---

# 🎯 Business Use Cases

* Smart City Analytics
* Urban Mobility Monitoring
* Traffic Analysis
* Emergency Response Tracking
* Weather Intelligence
* Data Lake Architecture Demonstration
* Real-Time Event Processing

---

# 📈 Future Enhancements

* Apache Airflow Orchestration
* AWS Redshift Data Warehouse
* Delta Lake Implementation
* Grafana Dashboards
* Real-Time Alerting
* Machine Learning Traffic Prediction
* Data Quality Framework

---

# 🎓 Skills Demonstrated

* Data Engineering
* Big Data Processing
* Apache Kafka
* PySpark Structured Streaming
* AWS S3
* AWS Glue
* Amazon Athena
* Data Lake Architecture
* Event-Driven Systems
* Docker
* Cloud Data Platforms

---

<div align="center">

## ⭐ Star this repository if you found it useful!

### Built by Mohammad Amanuddin

Aspiring Data Engineer | Kafka | PySpark | AWS | Data Lakes

</div>
