from kaggle.api.kaggle_api_extended import KaggleApi
import os


class KaggleConnect:
    def __init__(self, dataset_name: str, download_path: str):
        self.dataset_name = dataset_name
        self.download_path = download_path
        self.api = self._create_kaggle_connection()

    def _create_kaggle_connection(self):
        """Authenticate and return a Kaggle API connection."""
        api = KaggleApi()
        api.authenticate()
        return api

    def download_dataset(self):
        """Download and unzip the dataset, then return the first CSV file path."""
        os.makedirs(self.download_path, exist_ok=True)
        self.api.dataset_download_files(
            self.dataset_name, path=self.download_path, unzip=True
        )

        # Search for the CSV files in the directory
        csv_files = []
        for file in os.listdir(self.download_path):
            if file.endswith(".csv"):
                csv_files.append(os.path.join(self.download_path, file))

        if not csv_files:
            raise FileNotFoundError("No CSV files found in the downloaded dataset.")
        
        print(f"Found {len(csv_files)} CSV file(s):")
        for csv_file in csv_files:
            print(f"  - {os.path.basename(csv_file)}")
        
        return csv_files
