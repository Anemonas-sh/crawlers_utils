import os
import glob
import posixpath
from googleapiclient import discovery
from google.cloud import storage
from shutil import make_archive

def connect_to_storage(bucket_name):
    storage_client = storage.Client()
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

create_compressed_folder(base_name="/home/francamacdowell/Workspace/DataTour/crawlers_utils/", filename="test", base_dir="/home/francamacdowell/Downloads/books")