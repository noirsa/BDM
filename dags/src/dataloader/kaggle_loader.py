import time

from pathlib import Path
import os
from .base_loader import BaseDataLoader
import pandas as pd

class KaggleLoader(BaseDataLoader):
    """
        Loader for Kaggle datasets.
        Inherits from BaseDataLoader and uses the injected MinioClient.
    """
    def fetch_and_upload_csv(self, handle, file_name, name, **pandas_kwargs):
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
                    raise FileNotFoundError(f"Expected file {file_name} not found in {download_path}")
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

            metadata = {
                "source": "kaggle",
                "url": f"https://www.kaggle.com/datasets/{handle}",
                "file_name": file_name,
            }

            self.logger.info(f"Successfully loaded {file_name} into DataFrame.")
            timestamp = int(time.time())
            self.logger.info(f"Uploading {file_name} to MinIO...")
            self.upload_csv(
                df=df,
                object_name = f"{name}_{timestamp}.csv",
                metadata=metadata
            )

        except Exception as e:
            # We RAISE the error here because if the data fetch fails,
            # the entire pipeline should stop to prevent downstream errors.
            self.logger.exception(f"Error fetching data from kagglehub for {handle}: {e}")
            raise

    def fetch_and_upload_image(self,handle, name):
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

        categories = [d for d in os.listdir(root_path) if os.path.isdir(os.path.join(root_path, d))]

        for i, category in enumerate(categories):
            category_path = os.path.join(root_path, category)
            files = [f for f in os.listdir(category_path) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]

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
                                        "label": category
                                    })

            self.logger.info(f"Successfully ingested {category} of {name} into MinIO.")

        self.logger.info(f"Successfully uploaded {handle} to MinIO.")
