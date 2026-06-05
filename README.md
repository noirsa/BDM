# BDM (Big Data Management)

This repository is part of the first phase of the **BDM 25-26** project, focused on designing a **big data processing pipeline** before implementation. The goal is to define a high-level architecture showing how **structured, semi-structured, and unstructured data** flows from ingestion (e.g., CSV files, JSON APIs, images) into a landing zone (MinIO/S3), through processing and transformation stages (using Spark and Delta Lake), and finally to exploitation or consumption stages for analysis or downstream applications. The project contextualizes the problem in the **climate and environmental domain**, aiming to provide insights from historical and live datasets while demonstrating robust data management. Although some components, like Trusted and Exploitation Zones, are currently black boxes, the design lays a solid blueprint for future development, including handling large datasets, metadata management, and potential integration with machine learning workflows.

---

### Start Docker Containers

Use the following command to deploy MinIO, ..., containers.

```bash
docker compose up --build -d
```

### GPU-enabled Notebook / Airflow Worker

The default `docker-compose.yml` can run on machines without an NVIDIA GPU. For
image embedding in the Exploitation Zone, use the GPU override file so Jupyter
and the Airflow worker can access CUDA:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml build jupyter airflow-worker
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d --force-recreate jupyter airflow-worker
```

Verify that the running Jupyter container can see the GPU:

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml exec jupyter python -c "import torch; print(torch.__version__); print(torch.version.cuda); print(torch.cuda.is_available()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'NO GPU')"
```

Expected GPU result: `torch.cuda.is_available()` prints `True`.

If it prints `False`, the Exploitation image vectorization pipeline will still
fall back to CPU. Rebuild and recreate the containers after changing
`docker/jupyter/requirements.txt` or `docker/airflow/requirements.txt`; an
already-running container will not pick up new PyTorch packages or GPU access.

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

### Trusted Zone governance notes

The Trusted Zone DAGs use least-privilege service credentials for normal
pipeline access while keeping admin/root accounts available for maintenance.
RBAC bootstrap is wired into `docker-compose.yml`: MinIO policies/users,
MongoDB trusted writer, and ClickHouse trusted users are recreated from the
repository configuration when services are rebuilt from empty volumes.

Invalid data is not silently dropped. Structured cleaning writes rejected
summaries to `s3://trusted-zone/rejected/structured/`; semi-structured invalid
documents go to MongoDB collections named `<topic>_rejected`; unstructured image
metadata and transform failures go to
`s3://trusted-zone/rejected/unstructured/image/`.

The semi-structured production DAG intentionally reads date-partitioned Kafka
landing paths such as
`persistent-landing/semistructured/<topic>/<yyyymmdd>/`. Exploratory notebooks
may use aggregate JSON/testing files, so path differences between notebook and
DAG are expected.

---

## Execution Order
- **Jupyter token**: `jupyter`
### Landing Zone
- `landing_zone.ipynb`
- `temporal_landing.ipynb`  
- `persistent_landing.ipynb`  
- `persistent_landing_delta_lake.ipynb`
- `data_ingestion_streaming.ipynb`
- `persistent_landing_delta_lake_nonstructured.ipynb`
### Trusted Zone
- `trusted_zone.ipynb`
- `structured_data_cleaning.ipynb`  
- `semi-structured_data_cleaning.ipynb`  
- `unstructured_data_cleaning.ipynb`
- `catalogue_construction.ipynb`
