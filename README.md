# BDM Climate and Disaster Analytics Pipeline

This project implements a Big Data Management pipeline for climate, environmental, disaster-tweet, streaming, and image data. It demonstrates the full path from raw ingestion to curated trusted data, exploitation marts/vector search, and consumption outputs.

The pipeline can be run in two ways:

- **Airflow DAGs**: recommended for the final end-to-end demo.
- **Jupyter notebooks**: recommended for step-by-step report evidence and classroom inspection.

The notebooks and DAGs are intentionally mirrored, but notebooks do not import DAG helper functions.

---

## 1. Prerequisites

Install:

- Docker Desktop
- Docker Compose
- Git

Create the local environment file:

```powershell
copy .env.example .env
```

The default `.env.example` values are designed for the Docker Compose network. Do not commit real passwords or access keys.

---

## 2. Start The Stack

### Full Stack

This project provides two helper scripts for starting the full stack with automatic GPU detection and three Spark workers.

The startup scripts automatically detect whether NVIDIA GPU support is available:

- If `nvidia-smi` is available, the scripts use `docker-compose.gpu.yml`.
- If no NVIDIA GPU is detected, the scripts fall back to the default non-GPU stack.
- Both modes start three Spark workers with `--scale spark-worker=3`.

| Platform | Script |
|---|---|
| Windows PowerShell | `run.ps1` |
| Linux/macOS Bash | `run.sh` |

On Windows PowerShell:

```powershell
.\run.ps1
```

On Linux/macOS Bash:

```bash
chmod +x run.sh
./run.sh
```

### Optional Docker image/cache cleanup

The startup scripts support optional cleanup flags for reducing Docker image and build-cache disk usage.

These cleanup options are intended for cases where Docker images or build cache have grown too large. They do **not** delete Docker volumes, so persistent data such as databases, object storage, Kafka data, Superset metadata, Airflow metadata, and mounted data directories are preserved.

| Cleanup level | Windows PowerShell | Linux/macOS Bash | What it does |
|---|---|---|---|
| Light cleanup | `.\run.ps1 -Prune` | `./run.sh --prune` | Removes build cache and dangling images |
| Deep cleanup | `.\run.ps1 -DeepPrune` | `./run.sh --deep-prune` | Removes all unused images and build cache |

#### Examples

Start with light cleanup:

```powershell
.\run.ps1 -Prune
```

```bash
./run.sh --prune
```
> Note: The Bash script has not been fully tested on all Linux/macOS environments.

> Tip: If either startup script fails, you can always start the default non-GPU stack manually with the command below. This is the safest fallback option and should work on machines without GPU support:
>
> ```bash
> docker compose up --build -d --scale spark-worker=3
> ```

Useful local URLs:

| Service | URL | Notes |
|---|---|---|
| Jupyter | http://localhost:8888 | token: `jupyter` |
| Airflow | http://localhost:8080 | user/password: `airflow` / `airflow` |
| MinIO Console | http://localhost:9001 | credentials from `.env` |
| Spark Master UI | http://localhost:8082 | worker/application status |
| Kafka UI | http://localhost:8081 | topics and messages |
| ClickHouse UI | http://localhost:8123 | ClickHouse DB inspection |
| Mongo Express | http://localhost:8083 | MongoDB inspection |
| Attu | http://localhost:3000 | Milvus vector DB inspection |
| Superset | http://localhost:8088 | dashboard service |

### Spark Workers for Dedicated Workloads

This project uses three Spark workers by default so that different Spark applications can run without blocking each other:

- one worker for semi-structured processing
- one worker for regular/batch processing tasks
- one worker for the consumption dashboard / streaming workload

After startup, open http://localhost:8082 and confirm that three workers are registered.

> Note: Spark still schedules executors automatically across available workers. The three-worker setup provides enough capacity for the semi-structured, regular processing, and consumption workloads to run concurrently, but it does not pin each workload to a specific worker unless additional resource constraints are configured.

### GPU Optional

The default stack can run without a GPU.

If a working NVIDIA GPU environment is detected, the startup scripts use the GPU Compose override automatically:

```text
docker-compose.gpu.yml
```

If CUDA is unavailable or the GPU stack cannot start, use the default non-GPU fallback:

```bash
docker compose up --build -d --scale spark-worker=3
```

Verify GPU availability after startup:

```powershell
docker compose -f docker-compose.yml -f docker-compose.gpu.yml exec jupyter python -c "import torch; print(torch.cuda.is_available()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'NO GPU')"
```

If CUDA is unavailable inside the container, image vectorization falls back to CPU only when the service itself can start successfully.

### Script Encoding

Save the Bash script as:

```text
UTF-8 without BOM
LF line endings
```

Save the PowerShell script as:

```text
UTF-8
```

or, for older Windows PowerShell compatibility:

```text
UTF-8 with BOM
```

---

## 3. Airflow Run

Airflow DAGs are manual-trigger friendly. They also support automatic downstream triggering when an upstream DAG succeeds.

Main chain:

```text
ingest_dataset
  -> temporal_to_persistent
  -> write_deltalake
  -> trusted_zone
  -> exploitation_zone_structured
  -> consumption_zone_tweet_classifier

trusted_zone
  -> exploitation_zone_image_vectorization

trusted_zone_semistructured_daily
  -> exploitation_zone_semistructured
```

Important notes:

- `catchup=False` is used.
- DAGs are paused at creation, so enable/trigger the DAGs you want in the Airflow UI.
- You can manually run any individual DAG.
- After `trusted_zone`, the structured exploitation and image vectorization DAGs run sequentially to avoid Spark application contention. The semi-structured path is handled by `trusted_zone_semistructured_daily`, which triggers `exploitation_zone_semistructured`.
- `ingest_kafka_dataset` and `daily_kafka_data_catalog_update` are scheduled DAGs for streaming/semistructured landing catalogue support.

Recommended Airflow demo:

1. Open Airflow at http://localhost:8080.
2. Unpause the required DAGs.
3. Trigger `ingest_dataset`.
4. Watch the chain continue through trusted and exploitation.
5. Inspect final outputs in ClickHouse, MongoDB, MinIO, Milvus/Attu, and Superset/Postgres.

Check DAG import errors:

```powershell
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Expected output:

```text
No data found
```

---

## 4. Notebook Run

Open Jupyter:

```text
http://localhost:8888
token: jupyter
```

Run notebooks in this order.

### Landing Zone

1. `notebooks/Landing Zone/1. landing_zone.ipynb`
2. `notebooks/Landing Zone/2. temporal_landing.ipynb`
3. `notebooks/Landing Zone/3. persistent_landing.ipynb`
4. `notebooks/Landing Zone/4. persistent_landing_delta_lake.ipynb`
5. `notebooks/Landing Zone/5. data_ingestion_streaming.ipynb`
6. `notebooks/Landing Zone/6. persistent_landing_delta_lake_nonstructured.ipynb`

`5. data_ingestion_streaming.ipynb` contains a Kafka aggregation loop. It is normal for it not to finish by itself. Run it long enough to write `weather-barcelona.json` and `airquality-barcelona.json`, then interrupt the kernel and continue with notebook 6.

### Trusted Zone

1. `notebooks/Trusted Zone/1. trusted_zone.ipynb`
2. `notebooks/Trusted Zone/2. structured_data_cleaning.ipynb`
3. `notebooks/Trusted Zone/3. semi-structured_data_cleaning.ipynb`
4. `notebooks/Trusted Zone/4. unstructured_data_cleaning.ipynb`

The unstructured notebook includes trusted image catalogue evidence. There is no separate `catalogue_construction.ipynb`.

### Exploitation Zone

1. `notebooks/Exploitation Zone/1. exploitation_zone.ipynb`
2. `notebooks/Exploitation Zone/2. structured_data_exploitation.ipynb`
3. `notebooks/Exploitation Zone/3. semi-structured_data_exploitation.ipynb`
4. `notebooks/Exploitation Zone/4. unstructured_data_vectorization.ipynb`

Image vectorization can be slow on CPU. Use the GPU override if available.

### Consumption Zone

1. `notebooks/Consumption Zone/1. natural_disaster_tweet_classifier.ipynb`
2. `notebooks/Consumption Zone/2. spark_streaming_with_kafka_dash.ipynb`

The Spark streaming dashboard logic also exists as `scripts/consumption_streaming_dashboard.py`.
The `consumption_zone_streaming_dashboard_trigger` DAG runs hourly as a supervisor:
it checks the Spark application health and restarts the Python streaming job when
the dashboard stream is not active.

---

## 5. Outputs To Inspect

| Zone | Main Outputs |
|---|---|
| Landing | MinIO `landing-zone`, Delta file catalogue |
| Trusted structured | ClickHouse `bi_analytics.*` |
| Trusted semi-structured | MongoDB `trusted_zone_semi-structured.*` |
| Trusted unstructured | MinIO `trusted-zone/unstructured/image/`, `trusted-zone/file_catalog/` |
| Trusted rejected evidence | `trusted-zone/rejected/`, MongoDB rejected collections |
| Exploitation structured | ClickHouse `exploitation_analytics.dim_*`, `fact_*`, `bridge_*`, `mart_*` |
| Exploitation semi-structured | MongoDB `exploitation_zone_semi_structured.*` |
| Exploitation image | Milvus `image_vector_catalog`, MinIO `exploitation-zone/catalogue/image_vectorization/` |
| Consumption | MinIO classifier model artifacts, ClickHouse model metrics/model URIs, Postgres/Superset dashboard tables |

---

## 6. Reset Data For A Fresh Test

For a complete cold start, stop containers and remove volumes:

```powershell
docker compose down -v
docker compose up --build -d
docker compose up -d --force-recreate --scale spark-worker=3 spark-master spark-worker
```

This deletes runtime data in Docker volumes, including MinIO, MongoDB, ClickHouse, Milvus, Postgres, Kafka, and Airflow metadata. Source code and notebooks are not deleted.

If you only want to stop the live Kafka producer before a controlled test:

```powershell
docker compose stop kafka-producer
```

Restart it when needed:

```powershell
docker compose start kafka-producer
```

---

## 7. Governance Evidence

The report-facing governance configuration is in:

- `config/pipeline_manifest.yaml`
- `docs/governance_matrix.md`

These files document zones, DAGs, notebook paths, source/target systems, output assets, metadata fields, lineage fields, rejected/quarantine outputs, and RBAC roles.

RBAC smoke test:

```powershell
docker compose exec -T airflow-apiserver python /opt/airflow/scripts/verify_governance_access.py
```

Use write checks only when you intentionally want temporary test objects:

```powershell
docker compose exec -T airflow-apiserver python /opt/airflow/scripts/verify_governance_access.py --write-checks
```

---

## 8. Common Checks

Check running containers:

```powershell
docker compose ps
```

Check Spark workers:

```powershell
docker compose logs spark-worker --tail=100
```

Check Jupyter logs:

```powershell
docker compose logs jupyter --tail=100
```

Check Airflow import errors:

```powershell
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Read Kafka topics:

```powershell
docker exec -it bdm-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic weather-barcelona --from-beginning
docker exec -it bdm-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic airquality-barcelona --from-beginning
```

---

## 9. Notes For Markers

- The project supports structured, semi-structured, and unstructured data.
- The DAG chain demonstrates orchestration while preserving manual DAG execution.
- Notebook and DAG implementations are mirrored for evidence, but notebooks remain self-contained.
- Semi-structured notebook input differences are intentional: notebooks may use aggregation/testing output, while the DAG follows date-partitioned Kafka landing output.
- Trusted cleaning includes rejected/quarantine evidence and metadata/lineage fields.
- Exploitation outputs include catalogue, quality summary, and lineage records.
