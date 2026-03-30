import time

from datasets import load_dataset
import io
from .base_loader import BaseDataLoader


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

    def fetch_and_upload(self, path, name=None, split='train', file_type='csv'):



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

            metadata={"hf_path": path, "split": split}

            self.logger.info(f"Successfully loaded {path} into DataFrame.")

            if file_type == 'csv':
                df.to_csv(buffer, index=False, encoding='utf-8')
                content_type = 'text/csv'
            else:
                df.to_parquet(buffer, index=False)
                content_type = 'application/x-parquet'

            # Generate timestamped object name
            timestamp = int(time.time())
            self.logger.info(f"Uploading {path} to MinIO...")
            self.upload_csv(
                df=df,
                object_name = f"{name}_{timestamp}.csv",
                metadata=metadata,
                content_type=content_type
            )



        except Exception as e:
            self.logger.exception(f"Failed to process split {split} for {path}: {e}")
            # Depending on requirement, you might want to 'continue' or 'raise'
            raise