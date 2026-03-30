from datetime import datetime

import os
from pathlib import Path

from .base_loader import BaseDataLoader
import pandas as pd

class KaggleLoader(BaseDataLoader):

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

        return:
            df: Pandas DataFrame
                The loaded dataset.
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
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            self.logger.info(f"Uploading {file_name} to MinIO...")
            self.upload_csv(
                df=df,
                object_name = f"{name}_{timestamp}.csv",
                metadata=metadata
            )
            return df

        except Exception as e:
            # We RAISE the error here because if the data fetch fails,
            # the entire pipeline should stop to prevent downstream errors.
            self.logger.exception(f"Error fetching data from kagglehub for {handle}: {e}")
            raise