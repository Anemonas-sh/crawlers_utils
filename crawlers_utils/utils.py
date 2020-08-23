import os
import glob
import posixpath
import json
from googleapiclient import discovery
from google.cloud import storage


def save_file(file_path, data, bucket=None):
    with open(file_path, "w", encoding="utf-8") as f:
        print(json.dumps(data, ensure_ascii=False), file=f)
    if bucket is not None:
        upload_folder_to_bucket(bucket, file_path, file_path)


def connect_to_storage(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    return bucket


def upload_folder_to_bucket(bucket, local_path, bucket_path):
    blob = bucket.blob(bucket_path)
    blob.upload_from_filename(local_path)

