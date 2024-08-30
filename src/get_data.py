"""
    Stage where the data is downloaded from the S3 bucket.

    To access the bucket, a settings.conf file is required with valid
    credentials.
"""
from pathlib import Path
import os

from pyopensky.s3 import S3Client
from rich.console import Console

from utils.dvc.params import get_params


console = Console()
params = get_params('get_data')


if not os.path.exists(params['output_path']):
    os.makedirs(params['output_path'])

OUTPUT_PATH = Path(params['output_path'])


def main():
    s3 = S3Client()

    for obj in s3.s3client.list_objects("competition-data", recursive=True):
        # Check if the object is already in the output path
        if (OUTPUT_PATH / obj.object_name).exists():
            console.log(f"{obj.object_name} already exists in {OUTPUT_PATH}")
            continue
        console.log(f"Downloading {obj.object_name} from {obj.bucket_name}")
        s3.download_object(obj, filename=OUTPUT_PATH)
    console.log("Data downloaded successfully!")


if __name__ == "__main__":
    main()
