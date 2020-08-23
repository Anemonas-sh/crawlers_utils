import os
import glob
import posixpath
from googleapiclient import discovery
from google.cloud import storage
from shutil import make_archive

def connect_to_storage(bucket_name):
    storage_client = storage.Client()
    # Alternatively access Client from filename:
    # storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.get_bucket(bucket_name)
    return bucket


def upload_folder_to_bucket(bucket, local_path, bucket_path, recursive_upload=True):
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


def download_blob_from_bucket(bucket_name, bucket_path, source_name, path_to_save):
    """ Download blob from Storage bucket
    
    Parameters:
    bucket_name (str): Storage bucket name which will access. e.g.: "toureyes-data-lake"
    bucket_path (str): Path inside Google Storage bucket where your blob is find. e.g.:
    source_folder_name (str): Blob name to download from full storage path
    path_to_save (str): Full local path to download blob
    
    Returns:
    Nothing
    """

    bucket = connect_to_storage(bucket_name=bucket_name)
    blob = bucket.blob(bucket_path + "/" + source_name)
    print("Downloading: {}".format(bucket_path + "/" + source_name))
    blob.download_to_filename(path_to_save + "/" + source_name)
    
    print(
        "Blob {} downloaded to {}.".format(
            source_name, path_to_save
        )
    )


def create_compressed_folder(output_path: str = None, filename: str = None, compression_type="zip", base_dir: str = None):
    """
    base_dir: is the directory where we start archiving from
    compression_type can be:  “zip” (if the zlib module is available),
                                “tar”,
                                “gztar” (if the zlib module is available),
                                “bztar” (if the bz2 module is available), or
                                “xztar"
                                
    return: function returns the full path where archived/compressed folder was placed
    """
    
    return make_archive(base_name=output_path + filename, format=compression_type, base_dir=base_dir)
