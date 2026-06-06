from pathlib import Path
import os
from .base_loader import BaseDataLoader
from .validation import DatasetValidationSpec
from src.utils.time_anchor import logical_date_iso, logical_date_suffix
import pandas as pd

class KaggleLoader(BaseDataLoader):
    """
        Loader for Kaggle datasets.
        Inherits from BaseDataLoader and uses the injected MinioClient.
    """
    def fetch_and_upload_csv(
        self,
        handle,
        file_name,
        name,
        object_name=None,
        table_name=None,
        expected_rows=None,
        expected_columns=None,
        logical_date=None,
        **pandas_kwargs,
    ):
        """
        Fetches a dataset from Kaggle and uploads it to the MinIO landing zone.

        param:
            handle: str
                The Kaggle dataset identifier (e.g., 'username/dataset-name').
            file_name: str
                The specific CSV file name within the dataset to load.
            name: str
                The sanitized name to use for storage in MinIO (e.g., 'co2_emission').
            **pandas_kwargs: dict
                Additional arguments for pandas.read_csv (e.g., encoding='latin-1').

        Raises:
            KaggleApiError
                If the dataset or file cannot be found or accessed.
            ClientError
                If the upload to MinIO fails.
        """
        # Local import to avoid unnecessary overhead during DAG parsing
        import kagglehub
        from kagglehub import KaggleDatasetAdapter

        try:
            self.logger.info(f"Attempting to load dataset {handle} using kagglehub...")
            if pandas_kwargs:
                self.logger.info(f"Using custom pandas kwargs: {pandas_kwargs}")
                download_path = kagglehub.dataset_download(handle)
                csv_path = Path(download_path) / file_name
                if not csv_path.exists():
                    available = sorted(p.name for p in Path(download_path).glob("*.csv"))
                    raise FileNotFoundError(
                        f"Expected file {file_name} not found in {download_path}. "
                        f"Available CSV files: {available}"
                    )
                df = pd.read_csv(csv_path, **pandas_kwargs)

            # Use kagglehub to load the specific file directly into a DataFrame
            else:
                df = kagglehub.load_dataset(
                    KaggleDatasetAdapter.PANDAS,
                    handle,
                    file_name
                )

            if df.empty:
                raise ValueError(f"{file_name} is empty")

            if df.shape[1] == 0:
                raise ValueError("No columns found in dataset")

            validation = DatasetValidationSpec(
                dataset_name=name,
                source_file=file_name,
                expected_rows=expected_rows,
                expected_columns=expected_columns,
            )
            validation.validate_dataframe(df, self.logger)

            landing_name = object_name or f"{name}.csv"
            delta_table_name = table_name or Path(landing_name).stem
            landing_path = Path(landing_name)
            run_suffix = logical_date_suffix(logical_date)
            timestamped_object_name = f"{landing_path.stem}_{run_suffix}{landing_path.suffix or '.csv'}"
            metadata = {
                "source": "kaggle",
                "url": f"https://www.kaggle.com/datasets/{handle}",
                "file_name": file_name,
                "selection_strategy": "explicit_file_name",
                "dataset_name": name,
                "table_name": delta_table_name,
                "landing_name": landing_name,
                "logical_date": logical_date_iso(logical_date),
                "expected_rows": str(expected_rows or ""),
                "expected_columns": str(expected_columns or ""),
                "source_system": "kaggle",
                "ingestion_time": logical_date_iso(logical_date),
                "source_file_path": f"kaggle://{handle}/{file_name}",
                "validation_status": "valid",
                "schema_version": "landing_raw_v1",
                "owner": "data_engineering_team",
                "data_steward": "bdm_project_team",
                "data_classification": "public_environmental_analytics",
                "pii_flag": "no_direct_pii",
                "retention_policy": "course_project_retained_until_assessment_archive",
            }

            self.logger.info(f"Successfully loaded {file_name} into DataFrame.")
            self.logger.info(f"Uploading {file_name} to MinIO as {timestamped_object_name}...")
            self.upload_csv(
                df=df,
                object_name=timestamped_object_name,
                metadata=metadata
            )

        except Exception as e:
            # We RAISE the error here because if the data fetch fails,
            # the entire pipeline should stop to prevent downstream errors.
            self.logger.exception(f"Error fetching data from kagglehub for {handle}: {e}")
            raise

    def fetch_and_upload_image(self,handle, name, logical_date=None):
        """
        Downloads an image dataset from Kaggle and uploads to MinIO.

        param:
            handle : str
                Kaggle dataset identifier (e.g., 'jehanbhathena/weather-dataset').
            name : str
                The sanitized name/alias for the dataset.
            timestamp : int, optional

        Raises:
            KaggleApiError
                If the dataset or file cannot be found or accessed.
            ClientError
                If the upload to MinIO fails.
        """
        import kagglehub

        self.logger.info(f"Starting image ingestion for {handle}")
        download_path = kagglehub.dataset_download(handle)

        root_path = os.path.join(download_path, "dataset") if os.path.exists(os.path.join(download_path, "dataset")) else download_path

        categories = sorted(d for d in os.listdir(root_path) if os.path.isdir(os.path.join(root_path, d)))

        for i, category in enumerate(categories):
            category_path = os.path.join(root_path, category)
            files = sorted(f for f in os.listdir(category_path) if f.lower().endswith(('.jpg', '.jpeg', '.png')))

            for j, file_name in enumerate(files):
                file_path = os.path.join(category_path, file_name)
                ext = file_name.split('.')[-1]
                object_key = f"temporal-landing/image_{name}_{i}_{j}.{ext}"
                with open(file_path, "rb") as f:
                    self.upload_file(bucket_name="landing-zone",
                                     object_key=object_key,
                                     content=f.read(),
                                     content_type=f"image/{ext.lower()}",
                                     metadata= {
                                         "source": "kaggle",
                                         "url": f"https://www.kaggle.com/datasets/{handle}",
                                         "label": category,
                                         "dataset_name": name,
                                         "logical_date": logical_date_iso(logical_date),
                                         "source_system": "kaggle",
                                         "ingestion_time": logical_date_iso(logical_date),
                                         "source_file_path": f"kaggle://{handle}/{category}/{file_name}",
                                         "validation_status": "valid",
                                         "schema_version": "landing_raw_v1",
                                         "owner": "data_engineering_team",
                                         "data_steward": "bdm_project_team",
                                         "data_classification": "public_image",
                                         "pii_flag": "no_direct_pii",
                                         "retention_policy": "course_project_retained_until_assessment_archive",
                                     })

            self.logger.debug(f"Successfully ingested {category} of {name} into MinIO.")

        self.logger.info(f"Successfully uploaded {handle} to MinIO.")
