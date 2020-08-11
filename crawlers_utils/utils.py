import os
import glob
import posixpath
from googleapiclient import discovery
from google.cloud import storage


def upload_folder_to_bucket(bucket_name, local_path, bucket_path, recursive_upload=True):
    bucket = bucket_name
    for local_file in glob.glob(local_path + '/**'):
        if not os.path.isfile(local_file) and recursive_upload:
            upload_folder_to_bucket(bucket, local_file, bucket_path + "/" + os.path.basename(local_file))
        else:
            if '.' not in local_file:
                continue
            remote_path = posixpath.join(bucket_path, local_file[1 + len(local_path):])
            blob = bucket.blob(remote_path)
            print('sent {}'.format(local_file.split("/")[-1]))
            blob.upload_from_filename(local_file)


def connect_to_storage(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    return bucket
