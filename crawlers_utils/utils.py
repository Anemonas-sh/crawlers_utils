import os
import sys
import glob
import posixpath
import json
from googleapiclient import discovery
from google.cloud import storage
from shutil import make_archive
from datetime import datetime, timedelta
from time import time
from threading import Thread
from .constants import date_format


def run_crawler(start_date, end_date, out_dir, thread_count, init_crawler_func):
    out_dir = get_output_folder(start_date, end_date, out_dir)

    start_date = fail_recovery(start_date, out_dir)

    total_days = (end_date - start_date).days + 1
    thread_count = min(thread_count, total_days)

    threads = []
    for i in range(thread_count):
        lo = start_date + timedelta(days=i * total_days // thread_count + (i * total_days % thread_count != 0))
        hi = start_date + timedelta(days=(i + 1) * total_days // thread_count + ((i + 1) * total_days % thread_count != 0) - 1)
        t = Thread(target=init_crawler_func, args=(lo, hi, out_dir,), daemon=True)
        t.start()
        threads += [t]
    for t in threads:
        t.join()

    bucket = connect_to_storage("toureyes-data-lake")
    save_query(out_dir, bucket=bucket)


def fail_recovery(start_date, out_dir):
    try:
        while (start_date.strftime(date_format) + ".json") in os.listdir(out_dir):
            start_date += timedelta(days=1)
    except Exception as e:
        print("Fail recovery failed", e)
    print("Starting from", start_date.strftime(date_format))
    return start_date


def get_args():
    arguments = sys.argv
    start_date, end_date, debug, thread_count, estimate_level = None, None, False, 1, 2
    try:
        for i in range(len(arguments)):
            if arguments[i] == "--start-date":
                start_date = datetime.strptime(arguments[i + 1], "%m-%d-%Y")
            if arguments[i] == "--end-date":
                end_date = datetime.strptime(arguments[i + 1], "%m-%d-%Y")
            if arguments[i] == "--debug":
                debug = True
            if arguments[i] == "--estimate-level":
                estimate_level = int(arguments[i + 1])
            if arguments[i] == "--threads":
                thread_count = int(arguments[i + 1])
        if start_date is None or end_date is None:
            raise ValueError(start_date, end_date)
    except Exception as e:
        print("Couldn't get arguments", e)
        exit(0)
    return start_date, end_date, debug, thread_count, estimate_level


def get_output_folder(start_date, end_date, crawler_name):
    now, start, end = datetime.now().strftime(date_format), start_date.strftime(date_format), end_date.strftime(date_format)
    out_dir = posixpath.join(crawler_name, "%s_%s_%s" % (now, start, end))

    try:
        os.makedirs(out_dir) # makes sure that queries folder will exist
    except Exception as e:
        print(e)
        pass

    return out_dir


def print_end_estimate(start_time, index, total, start_date_time, tabs, estimate_level):
    if estimate_level < tabs:
        return
    estimate = int((time() - start_time) / index * (total - index))
    print("%s%d of %d (started at: %s, estimated to end at: %s) (%d hours, %d minutes and %d seconds)"
          % ("\t" * tabs, index, total,
             start_date_time.strftime(date_format),
             (start_date_time + timedelta(seconds=estimate)).strftime(date_format),
             estimate // 3600, estimate % 3600 // 60, estimate % 60), sep="")


def save_file(file_path, data, bucket=None):
    with open(file_path, "w", encoding="utf-8") as f:
        print(json.dumps(data, ensure_ascii=False), file=f)
    if bucket is not None:
        upload_folder_to_bucket(bucket, file_path, file_path)


def connect_to_storage(bucket_name):
    storage_client = storage.Client()
    # Alternatively access Client from filename:
    # storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.get_bucket(bucket_name)
    return bucket


def upload_folder_to_bucket(bucket, local_path, bucket_path):
    try:
        blob = bucket.blob(bucket_path)
        blob.upload_from_filename(local_path)
    except Exception as e:
        print('An error ocurred on file upload', e)
        pass


def download_blob_from_bucket(bucket_name, source_path, path_to_save):
    """ Download blob from Storage bucket

    Parameters:
    bucket_name (str): Storage bucket name which will access. e.g.: "toureyes-data-lake"
    source_path (str): Blob name to download from full storage path
    path_to_save (str): Full local path to download blob

    Returns:
    Nothing
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_path)
    assert blob.exists()
    print("Downloading: {}".format(source_path))
    blob.download_to_filename(path_to_save)

    print(
        "Blob {} downloaded to {}.".format(
            source_path, path_to_save
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
                                
    Returns:
    (str): Returns the full path where archived/compressed folder was placed
    """

    return make_archive(base_name=output_path + filename, format=compression_type, root_dir=base_dir)


def save_query(output_folder: str = None, bucket: str = None):
    compressed_folder_path = create_compressed_folder(output_path=output_folder, filename="", base_dir=output_folder)
    if bucket is not None:
        upload_folder_to_bucket(bucket, compressed_folder_path, output_folder)