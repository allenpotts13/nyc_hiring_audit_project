import os
from minio import Minio

def get_minio_client():
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_EXTERNAL_URL = os.getenv("MINIO_EXTERNAL_URL")
    return Minio(
        MINIO_EXTERNAL_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )