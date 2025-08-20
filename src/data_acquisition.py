import os
from dotenv import load_dotenv
from utils.logger import setup_logger
from io import BytesIO
import requests
from minio import Minio
from minio.error import S3Error
import pandas as pd
from utils.minio_client import get_minio_client

logger = setup_logger(__name__, "logs/data_acquisition.log")

load_dotenv()

def api_call(url, filename):
    logger.info(f"Downloading JSON data from {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()

        df = pd.DataFrame(json_data)
        temp_parquet_path = os.path.join("/tmp", filename.replace(".json", ".parquet"))

        df.to_parquet(temp_parquet_path, index=False)
        logger.info(f"Saved Parquet file to {temp_parquet_path}")

        minio_client = get_minio_client()
        bucket_name = os.getenv("MINIO_BUCKET_NAME")
        minio_path = f"raw/{os.path.basename(temp_parquet_path)}"

        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        minio_client.fput_object(
            bucket_name,
            minio_path,
            temp_parquet_path,
            content_type="application/octet-stream"
        )
        logger.info(f"Parquet file uploaded to MinIO at {bucket_name}/{minio_path}")

        os.remove(temp_parquet_path)
        return True
    except Exception as e:
        logger.error(f"Failed to process JSON and upload Parquet to MinIO: {e}")
        return False
    
def convert_excel_minio_to_parquet():
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    excel_path = os.getenv("RAW_LIGHTCAST_FILE")
    minio_client = get_minio_client()

    response = minio_client.get_object(bucket_name, excel_path)
    excel_bytes = BytesIO(response.read())
    excel_file = pd.ExcelFile(excel_bytes)

    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=2)

        if df.empty or df.shape[1] < 2:
            continue

        df = df.applymap(str)
        safe_sheet_name = sheet_name.replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        parquet_minio_path = f"raw/{safe_sheet_name}.parquet"
        minio_client.put_object(
            bucket_name,
            parquet_minio_path,
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        print(f"Uploaded {parquet_minio_path} to MinIO")

def main():
    logger.info("=" * 60)
    logger.info("STARTING DATA ACQUISITION")
    logger.info("=" * 60)

    convert_excel_minio_to_parquet()

    urls_and_files = [
        (os.getenv("PAYROLL_DATA_URL"), "Citywide_Payroll_Data.json"),
        (os.getenv("JOBS_POSTINGS_URL"), "Jobs_NYC_Postings_Data.json"),
    ]
    success = True
    for url, filename in urls_and_files:
        logger.info(f"URL for {filename}: {url}")
        if url:
            result = api_call(url, filename)
            success = success and result
    return success


if __name__ == "__main__":
    success = main()
    if success:
        logger.info("Data acquisition completed successfully.")
    else:
        logger.error("Data acquisition completed with errors.")