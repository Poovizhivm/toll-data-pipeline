# Toll Data Pipeline

An end-to-end Data Engineering / DataOps project demonstrating batch ETL processing, workflow orchestration with Apache Airflow, real-time streaming data ingestion with Apache Kafka, and database persistence in MySQL.

---

## 🏗️ Architecture

```text
                    BATCH PIPELINE
                         │
                         ▼
                  Raw toll datasets
   (vehicle-data.csv, tollplaza-data.tsv, payment-data.txt)
                         │
                         ▼
                     Shell ETL
                         │
                         ▼
               Airflow orchestration
                         │
                         ▼
               transformed_data.csv


                    STREAMING PIPELINE
                         │
                         ▼
              Toll Traffic Generator
                         │
                         ▼
                       Kafka
                         │
                         ▼
                toll_traffic topic
                         │
                         ▼
              Streaming Data Reader
                         │
                         ▼
                       MySQL
                         │
                         ▼
                   toll_db.toll_data
```

---

## 🛠️ Technologies

- **Apache Airflow 2.10.5**: Workflow orchestration and batch ETL DAG execution.
- **Apache Kafka & Zookeeper (Confluent cp-kafka/cp-zookeeper 7.7.7)**: Real-time distributed event streaming.
- **MySQL 8.0**: Relational database storage for streaming toll traffic data.
- **PostgreSQL 16**: Metadata database for Airflow.
- **Docker & Docker Compose**: Containerized multi-service infrastructure.
- **Python 3.10**: Streaming producer and consumer scripts (`kafka-python`, `mysql-connector-python`).
- **Shell Scripting (Bash)**: Text processing and transformations (`cut`, `tr`, `paste`, `awk`).

---

## 📁 Project Structure

```text
toll-data-pipeline/
├── airflow/
│   └── dags/
│       └── ETL_toll_data.py          # Airflow DAG orchestrating batch ETL
├── consumer/
│   └── streaming_data_reader.py      # Kafka consumer writing to MySQL
├── data/
│   ├── vehicle-data.csv              # Raw vehicle data (CSV)
│   ├── tollplaza-data.tsv            # Raw toll plaza data (TSV)
│   ├── payment-data.txt              # Raw payment data (Fixed-width)
│   ├── tolldata.tgz                  # Source tarball archive
│   └── transformed_data.csv          # Final transformed dataset
├── database/
│   └── init.sql                      # Database and table initialization
├── kafka/
│   └── toll_traffic_generator.py     # Python streaming traffic producer
├── shell/
│   └── Extract_Transform_data.sh     # Bash ETL pipeline script
├── docker-compose.yml                # Multi-container service definitions
├── .gitignore
└── README.md
```

---

## 🚀 Execution & Verification

### 1. Start Infrastructure Services

```bash
docker compose up -d
```

Verify that all services are healthy and running:
```bash
docker compose ps
```

### 2. Phase 1 — Shell ETL
Extracts specific fields from CSV, TSV, and fixed-width files, merges them, and transforms vehicle type to uppercase:
```bash
bash shell/Extract_Transform_data.sh
```
*Output: `data/transformed_data.csv`*

### 3. Phase 2 — Airflow Orchestration
Orchestrates the batch workflow using the DAG tasks:
`unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data`

Test the DAG execution:
```bash
docker compose run --rm airflow-webserver airflow dags test ETL_toll_data
```

Access the Airflow Web UI at [http://localhost:8080](http://localhost:8080) (Credentials: `admin` / `admin`).

### 4. Phase 3 & 4 — Real-Time Streaming Pipeline

1. **Verify Kafka Broker and Topic:**
   ```bash
   docker exec toll_kafka kafka-topics --bootstrap-server kafka:29092 --list
   ```

2. **Start the Consumer (reads from Kafka and writes to MySQL):**
   ```bash
   python consumer/streaming_data_reader.py
   ```

3. **Start the Producer (generates toll events):**
   ```bash
   python kafka/toll_traffic_generator.py
   ```

4. **Verify Records in MySQL:**
   ```bash
   docker exec toll_mysql mysql -utoll_user -ptoll_password toll_db -e "SELECT COUNT(*) FROM toll_data; SELECT * FROM toll_data ORDER BY id DESC LIMIT 5;"
   ```
