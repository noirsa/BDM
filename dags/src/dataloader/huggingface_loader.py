from datasets import load_dataset
import io
from pathlib import Path
from .base_loader import BaseDataLoader
from .validation import DatasetValidationSpec
from src.utils.time_anchor import logical_date_iso, logical_date_suffix


class HuggingfaceDataLoader(BaseDataLoader):
    """
        Loader for Hugging Face datasets.
        Inherits from BaseDataLoader and uses the injected MinioClient.

        param:
        path : str
            The Hugging Face dataset identifier (e.g., 'stanfordnlp/imdb').
        name : str, optional
            The specific subset/configuration of the dataset (e.g., 'en').
        splits : list of str, default ['train']
            A list of dataset splits to fetch (e.g., ['train', 'test', 'validation']).
        file_type : {'csv', 'parquet'}, default 'csv'
            The target storage format. Determines the conversion logic and
            Content-Type for MinIO.

    """

    def fetch_and_upload(
        self,
        path,
        name=None,
        split='train',
        file_type='csv',
        object_name=None,
        table_name=None,
        expected_rows=None,
        expected_columns=None,
        logical_date=None,
    ):



        self.logger.info(f"Starting HF ingestion for {path}, split: {split}")

        try:
            self.logger.info(f"Processing split: {split}")

            # Load the specific split
            dataset = load_dataset(path, split=split)


            # 3. Data Conversion
            buffer = io.BytesIO()
            df = dataset.to_pandas()


            if df.empty:
                raise ValueError(f"{path} is empty")

            if df.shape[1] == 0:
                raise ValueError("No columns found in dataset")

            validation = DatasetValidationSpec(
                dataset_name=name or path,
                source_file=path,
                expected_rows=expected_rows,
                expected_columns=expected_columns,
            )
            validation.validate_dataframe(df, self.logger)

            default_suffix = "csv" if file_type == "csv" else "parquet"
            landing_name = object_name or f"{name}.{default_suffix}"
            delta_table_name = table_name or Path(landing_name).stem
            landing_path = Path(landing_name)
            run_suffix = logical_date_suffix(logical_date)
            timestamped_object_name = f"{landing_path.stem}_{run_suffix}{landing_path.suffix or '.csv'}"

            metadata = {
                "source": "huggingface",
                "hf_path": path,
                "split": split,
                "dataset_name": name or path,
                "table_name": delta_table_name,
                "landing_name": landing_name,
                "logical_date": logical_date_iso(logical_date),
                "expected_rows": str(expected_rows or ""),
                "expected_columns": str(expected_columns or ""),
                "source_system": "huggingface",
                "ingestion_time": logical_date_iso(logical_date),
                "source_file_path": f"huggingface://{path}/{split}",
                "validation_status": "valid",
                "schema_version": "landing_raw_v1",
                "owner": "data_engineering_team",
                "data_steward": "bdm_project_team",
                "data_classification": "public_text_analytics",
                "pii_flag": "possible_user_mentions",
                "retention_policy": "course_project_retained_until_assessment_archive",
            }

            self.logger.info(f"Successfully loaded {path} into DataFrame.")

            if file_type == 'csv':
                self.logger.info(f"Uploading {path} to MinIO as {timestamped_object_name}...")
                self.upload_csv(
                    df=df,
                    object_name=timestamped_object_name,
                    metadata=metadata,
                    content_type='text/csv'
                )
            else:
                df.to_parquet(buffer, index=False)
                object_key = f"temporal-landing/{timestamped_object_name}"
                self.logger.info(f"Uploading {path} to MinIO as {object_key}...")
                self.upload_file(
                    bucket_name="landing-zone",
                    object_key=object_key,
                    content=buffer.getvalue(),
                    content_type='application/x-parquet',
                    metadata=metadata,
                )



        except Exception as e:
            self.logger.exception(f"Failed to process split {split} for {path}: {e}")
            # Depending on requirement, you might want to 'continue' or 'raise'
            raise
