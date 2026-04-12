# BDM (Big Data Management)

This repository is part of the first phase of the **BDM 25-26** project, focused on designing a **big data processing pipeline** before implementation. The goal is to define a high-level architecture showing how **structured, semi-structured, and unstructured data** flows from ingestion (e.g., CSV files, JSON APIs, images) into a landing zone (MinIO/S3), through processing and transformation stages (using Spark and Delta Lake), and finally to exploitation or consumption stages for analysis or downstream applications. The project contextualizes the problem in the **climate and environmental domain**, aiming to provide insights from historical and live datasets while demonstrating robust data management. Although some components, like Trusted and Exploitation Zones, are currently black boxes, the design lays a solid blueprint for future development, including handling large datasets, metadata management, and potential integration with machine learning workflows.

---

### Start Docker Containers

Use the following command to deploy MinIO, ..., containers.

```bash
docker compose up --build -d
```

### Read Weather-Barcelona

```bash
docker exec -it bdm-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic weather-barcelona --from-beginning
```

### Read Airquality-Barcelona

```bash
docker exec -it bdm-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic airquality-barcelona --from-beginning
```

### Airflow workflow

* **Credentials**
  - **Username**: `airflow`
  - **Password**: `airflow`
* **infra_daily_integrity_check**: Runs automatically every day. However, it can be **manually triggered** in the event of a failure.
* **Ingestion Pipeline**: 
    1.  `ingest_dataset` must be triggered manually.
    2.  Once complete, it automatically triggers `temporal_to_persistent`.
    3.  Finally, `temporal_to_persistent` triggers `write_deltalake`.
* **Airflow Tasks**:
    * Both `daily_kafka_data_catalog_update` and `ingest_kafka_dataset` are scheduled to run when the Airflow service is active.
    * **Frequency**: The catalog update runs **once daily**, while the dataset ingestion runs **every 5 minutes** by default.
