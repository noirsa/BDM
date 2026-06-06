# Data Governance Evidence Matrix

This document turns the project governance requirements into auditable evidence.
The implementation is intentionally lightweight because the project focus is big
data management rather than an enterprise governance platform.

## Governance Scope

| Area | Implementation evidence | Notes |
| --- | --- | --- |
| RBAC and least privilege | MinIO writer/reader policies, MongoDB trusted writer, ClickHouse trusted/consumption users, Milvus writer/reader users | Admin/root/analytics accounts are preserved for maintenance only. |
| Metadata and catalogue | Trusted catalogue summary, exploitation metadata fields, consumption metric/log metadata | Catalogue is a minimal project catalogue, not DataHub/Atlas. |
| Lineage | `source_system`, `source_file_path`, `source_assets`, `created_at`, `ingestion_time`, `schema_version` | Metadata lineage is used instead of a visual lineage graph. |
| Data quality | Dataset-specific Trusted Zone cleaning rules, invalid record quarantine | Invalid data is excluded from business trusted assets. |
| Reproducibility | Compose-mounted RBAC/init scripts and cold-start checklist | Full cold-start should be evidenced before final delivery. |

## RBAC Matrix

| System | Admin/root preserved | Writer/service user | Reader/service user | Normal pipeline usage | Bootstrap evidence |
| --- | --- | --- | --- | --- | --- |
| MinIO | `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | `MINIO_WRITER_ACCESS_KEY` | `MINIO_READER_ACCESS_KEY` | Landing, Trusted, Exploitation object IO uses service roles where configured | `init-scripts/minio-rbac.sh`, `config/minio_policies/*.json` |
| ClickHouse | `analytics` | `trusted_structured_writer`, `consumption_service` | `trusted_structured_reader` | Trusted structured writes use trusted writer; consumption reads/writes use consumption user | `init-scripts/clickhouse-trusted-users.xml` |
| MongoDB | `mongo` root user | `trusted_semistructured_writer` | Not separately required for current DAGs | Trusted semi-structured writes use trusted writer | `init-scripts/mongo-rbac.js` |
| Milvus | `root` | `bdm_vector_writer` | `bdm_vector_reader` | Image vectorization writes with writer; search/inspection reads with reader | `init-scripts/milvus-rbac.py` |
| Airflow | `airflow` UI user | N/A | N/A | Orchestrates DAGs and injects env-based service credentials | `docker-compose.yml` |

## Policy Access Matrix

| Principal | Allowed | Not intended for | Evidence |
| --- | --- | --- | --- |
| `bdm_writer` | Put/list/delete project objects in landing/trusted/exploitation buckets | Human admin operations | `landing_writer_policy.json` |
| `bdm_reader` | List/read project buckets and objects | Object writes/deletes | `landing_reader_policy.json` |
| `trusted_structured_writer` | Create/insert/select trusted structured tables in `bi_analytics` | Cross-system admin actions | ClickHouse XML grants |
| `trusted_structured_reader` | Show/select trusted structured tables in `bi_analytics` | Writes, DDL, exploitation outputs | ClickHouse XML grants |
| `consumption_service` | Select `exploitation_analytics.fact_tweet_features`; create/insert/select consumption metric/log tables | Trusted table writes or admin maintenance | ClickHouse XML grants |
| `trusted_semistructured_writer` | Read/write trusted MongoDB semi-structured collections | MongoDB admin operations | Mongo init JS |
| `bdm_vector_writer` | Milvus collection administration/upsert for image vectors | Root-level maintenance | Milvus RBAC script |
| `bdm_vector_reader` | Milvus vector search/read access | Collection mutation | Milvus RBAC script |

## Dataset-Specific Trusted Cleaning Rules

| Dataset | Expected schema focus | Deduplication key | Standardization and casting | Invalid/rejected handling |
| --- | --- | --- | --- | --- |
| `co2_emission_by_vehicles` | make, model, vehicle class, engine/fuel/emission measures | Full row; business keys include make/model/class/transmission/fuel type | Lowercase strings, normalized headers, integer casts for cylinders/mpg/emissions, negative numeric values masked to null | Missing required columns skip dataset with rejected event; missing required row values are quarantined |
| `global_warming_dataset` | country, year, climate/economic measures | Full row; business key country/year | Lowercase strings, integer year/events, non-negative measures, year constrained to 1900-2100 | Missing country/year rows are rejected; invalid ranges become null before required checks |
| `natural_disaster_tweets` | id, text, label, hashtags, emojis | Unique `id` | id cast to string, hashtags/emojis normalized into arrays, strings lowercased/trimmed | Missing id rows are rejected; duplicate ids fail quality validation |
| `temperature_change` | area, months, year, unit, value, flags | Full row; business key area/months/year | Month labels standardize dash spacing, unit normalized to degree C, integer year/year_code, year constrained to 1961-2100 | Missing area/month/year rows are rejected |
| `weather_barcelona` | time, temperature | Source event fields | Keys normalized, strings trimmed/lowercased | Missing required fields go to Mongo rejected collection |
| `airquality_barcelona` | id, name, locality, coordinates | Source station fields | Nested snapshots flattened; country/owner/provider/coordinates exposed as stable fields | Missing required fields go to Mongo rejected collection |
| Image catalogue | file path, file size, corruption flag, extension, md5 | md5 | Valid files standardized to RGB PNG 512x512 | Missing path, empty source, corrupted files, unsupported extensions, and transform failures go to rejected image catalogue |

## Rejected and Quarantine Evidence

| Data type | Rejected location | Rejected condition examples | Review fields |
| --- | --- | --- | --- |
| Structured | `s3://trusted-zone/rejected/structured/` | Missing required columns, missing required row values | reason, source file path, required columns, rejected count, sample records |
| Semi-structured | MongoDB `<topic>_rejected`; MinIO event logs for invalid payloads | Missing required JSON fields, invalid JSON payload | reason, record keys, raw record, source path, rejected_at |
| Unstructured | `s3://trusted-zone/rejected/unstructured/image/` | Corrupted image, missing path, zero-size file, unsupported extension, failed transform | id, source path, reason, rejected_at, schema version |

## Catalogue and Lineage Fields

The Trusted catalogue summary includes operational metadata:

- `zone`
- `asset_type`
- `storage_system`
- `source_type`
- `dataset_name`
- `target_name`
- `trusted_location`
- `record_count`
- `file_count`
- `column_or_key_count`
- `schema_json`
- `source_path`
- `source_location`
- `schema_version`
- `validation_status`
- `created_at`
- `updated_at`
- `upstream_zone`
- `downstream_usage`
- `logical_date`
- `source_system`
- `ingestion_time`
- `source_file_path`

The catalogue also includes lightweight policy metadata:

- `owner`
- `data_steward`
- `data_classification`
- `pii_flag`
- `retention_policy`

Exploitation and Consumption outputs use `source_assets` instead of forcing a
single `source_file_path`, because downstream assets can combine multiple
trusted tables, collections, or streaming sources.

## Cold-Start Reproducibility Checklist

Run this only when it is acceptable to rebuild local state.

1. Stop services and remove volumes.
   ```bash
   docker compose down -v
   ```
2. Rebuild and start the stack.
   ```bash
   docker compose up --build -d
   ```
3. Verify service health and ClickHouse authenticated healthcheck.
   ```bash
   docker compose ps
   docker compose exec -T clickhouse clickhouse-client --user analytics --password analytics_secret --query "SELECT version()"
   ```
4. Verify RBAC bootstrap.
   ```bash
   docker compose exec -T clickhouse clickhouse-client --user consumption_service --password consumption_service_password --query "SELECT currentUser()"
   docker compose logs minio-rbac mongo-rbac milvus-rbac
   ```
5. Run the access smoke test.
   ```bash
   docker compose exec -T airflow-apiserver python /opt/airflow/scripts/verify_governance_access.py
   ```
6. Trigger required DAGs in order and capture successful runs.
   - Landing/persistent ingestion DAGs
   - `trusted_zone`
   - `exploitation_zone_structured`
   - `exploitation_zone_semistructured`
   - `exploitation_zone_image_vectorization`
   - `consumption_zone_tweet_classifier`
   - `consumption_zone_streaming_dashboard_trigger`

Evidence to capture for the report:

- `docker compose ps`
- Airflow import errors show `No data found`
- Successful DAG run screenshots or CLI output
- Row counts from trusted/exploitation/consumption outputs
- RBAC smoke test output with no secrets
