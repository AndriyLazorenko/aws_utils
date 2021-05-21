import functools
import math
import os
import glob
import re
import shutil
import sys
import tarfile
import logging
import threading
from fractions import Fraction
from time import sleep
from typing import List

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, EndpointConnectionError
import botocore

BUCKET_NAME = 'sagemaker-us-east-1-113379206287'
REGION_NAME = "us-east-1"


# from main_training import logger

def retry(exception: Exception, tries: int):
    def decorator_repeat(func):
        @functools.wraps(func)
        def wrapper_repeat(*args, **kwargs):
            for i in range(tries):
                try:
                    return func(*args, **kwargs)
                except exception as exc:
                    print(f"\nThe function {func.__name__} has raised an Exception:"
                          f"{exc}\nTries left: {tries - i}")
                    timeout = math.factorial(i)
                    sleep(timeout)
            return func(*args, **kwargs)

        return wrapper_repeat

    return decorator_repeat


class S3ORM:
    def __init__(self,
                 bucket_name: str = '',
                 temp_folder: str = 'temp',
                 debug: bool = False):
        """
        Class facilitating connection to and from S3

        Args:
            bucket_name: name of s3 bucket
            temp_folder: temp local folder to store the files into
        """
        self.bucket_name = bucket_name
        self.temp_folder = temp_folder
        self.debug = debug

    def list_all_files_in_directory(self, s3_directory: str):
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(self.bucket_name)
        return [object_summary.key for object_summary in my_bucket.objects.filter(Prefix=s3_directory)]

    @retry(ClientError, tries=2)
    def download_file_from_s3(self, s3_filepath: str) -> str:
        """

        Args:
            s3_filepath: str: path to file in S3

        Returns:
            local_filepath: str: path to the file locally

        """
        temp_filename = s3_filepath.split("/").pop(-1)
        local_filepath = os.path.join(self.temp_folder, temp_filename)
        os.makedirs(self.temp_folder, exist_ok=True)
        if os.path.isfile(local_filepath) and self.debug:
            return local_filepath

        s3 = boto3.client('s3')
        print(f"Starting download of {s3_filepath}...")
        s3.download_file(self.bucket_name, s3_filepath, local_filepath)
        print("Download complete")

        return local_filepath

    def download_files_from_s3(self, s3_folder: str):
        filelist = self.list_all_files_in_directory(s3_folder)
        for f in filelist:
            print(f)
            self.download_file_from_s3(f)

    @staticmethod
    def train_test_file_split(file_dir: str, split_ratio: float = 0.9) -> List[str]:
        f = 1 - Fraction(str(split_ratio) + '\t\n')
        os.makedirs(os.path.join(file_dir, 'test'), exist_ok=True)
        os.makedirs(os.path.join(file_dir, 'train'), exist_ok=True)
        for subdir, dirs, files in os.walk(file_dir):
            for ind, file in enumerate(files):
                try:
                    if ind % f.denominator == f.numerator:
                        shutil.move(os.path.join(file_dir, file), os.path.join(file_dir, 'test', file))
                    else:
                        shutil.move(os.path.join(file_dir, file), os.path.join(file_dir, 'train', file))
                except FileNotFoundError as err:
                    continue
        return os.path.join(file_dir, 'train'), os.path.join(file_dir, 'test')

    def unarchive_to_s3(self,
                        path_from: str,
                        path_to: str = None,
                        delete_source: bool = False,
                        is_train_test_split: bool = False,
                        train_test_split_ratio: float = 0.9):
        local_out_folder = self.unarchive_from_s3_locally(path_from)
        print(f"Unarchived files are in {local_out_folder}")

        if is_train_test_split:
            local_out_folders = self.train_test_file_split(local_out_folder, train_test_split_ratio)
            for folder in local_out_folders:
                path_to = os.path.join("/".join(path_from.split(".")[0].split("/")[:-1]))
                self.upload_files_to_s3(s3_folder=path_to,
                                        local_folder_path=folder)
        else:
            if path_to is None:
                path_to = "/".join(path_from.split(".")[0].split("/")[:-1])
            print(f"Uploading files to s3 into the following path {path_to}")
            self.upload_files_to_s3(s3_folder=path_to,
                                    local_folder_path=local_out_folder)
        if delete_source:
            shutil.rmtree(self.temp_folder)

    def unarchive_from_s3_locally(self, path_from: str):
        implemented_archive_types = ["zip", 'rar']
        archive_type = path_from.split(".")[-1]
        if archive_type not in implemented_archive_types:
            raise NotImplementedError(f"The method is not implemented for {archive_type} type of archives. Feel free to"
                                      f" contribute!")

        local_fpath = self.download_file_from_s3(path_from)
        local_out_folder = local_fpath.split(".")[0]
        if archive_type == "zip":
            import zipfile
            with zipfile.ZipFile(local_fpath, 'r') as zip_ref:
                zip_ref.extractall(self.temp_folder)
        elif archive_type == "rar":
            from rarfile import RarFile
            with RarFile(local_fpath) as rf:
                rf.extractall(path=local_out_folder)
        print("File extracted!")
        return local_out_folder

    @staticmethod
    @retry(ClientError, tries=5)
    @retry(EndpointConnectionError, tries=5)
    def upload_file_to_s3(path_from: str,
                          path_to: str,
                          bucket_name: str
                          ):
        """
        Uploads a single file to S3
        Args:
            path_from: str: Path on local machine to upload files from
            bucket_name: str: destination bucket on S3
            path_to: str: Path on S3 to upload files to

        Returns:

        """
        s3_resource = boto3.resource('s3')
        config = TransferConfig(multipart_threshold=1024 * 100, max_concurrency=10,
                                multipart_chunksize=1024 * 100, use_threads=True)
        s3_resource.meta.client.upload_file(path_from, bucket_name, path_to,
                                            Config=config,
                                            Callback=ProgressPercentage(path_from)
                                            )

    def upload_files_to_s3(self,
                           s3_folder: str,
                           local_folder_path: str = 'temp',
                           ):
        """
        Uploads all the files present in directory and all subdirectories of that directory to S3

        Args:
            s3_folder: str: destination folder path on s3
            local_folder_path: str: a folder from which to move files on s3

        Returns:

        """
        for subdir, dirs, files in os.walk(local_folder_path):
            for file in files:
                path_from = os.path.join(subdir, file)
                suffix = path_from.split("/")[1:]
                s3_folder_list = s3_folder.split("/")
                s3_folder_list.extend(suffix)
                path_to = os.path.join(*s3_folder_list)
                S3ORM.upload_file_to_s3(path_from=path_from, bucket_name=self.bucket_name, path_to=path_to)


def multi_part_upload_with_s3(args):
    """
    move model output to s3
    Parameters
    ----------
    args: parameters

    Returns
    -------

    """
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 100, max_concurrency=10,
                            multipart_chunksize=1024 * 100, use_threads=True)

    file_path = args['output_path'] + "/**/*.*"
    key_folder = 'NLP-core/model_output'
    s3_resource = boto3.resource('s3')
    for path in glob.glob(file_path, recursive=True):

        recursive_target_path = os.path.dirname(path).split(os.path.dirname(args['output_path']), 1)[1]
        key_path = os.path.join(key_folder + recursive_target_path, os.path.basename(path))

        try:
            s3_resource.meta.client.upload_file(path, BUCKET_NAME, key_path,  # Body=f,
                                                ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/pdf'},
                                                Config=config,
                                                Callback=ProgressPercentage(path)
                                                )
        except ClientError as e:
            logging.error(e)


class S3Utils:
    def __init__(self,
                 bucket_name: str,
                 temp_folder: str = 'temp',
                 temp_filename: str = 'model.tar.gz',
                 prefix: str = 'NLP-core/model_output/',
                 s3_filepath=None,
                 is_full_extraction: bool = True):
        """
        Class facilitating unarchiving reports on S3 using local machine. Is able to unarchive all the model outputs
        on S3

        Args:
            bucket_name: str: bucket name on S3
            temp_folder: str: temp folder to store data in
            temp_filename: str: filename of tar to be stored locally
            s3_filepath: str: filepath of s3 report. If left unchecked, will unarchive all the model reports present
            on s3 in directory of model results
            is_full_extraction: bool: if there is a need to extract all the files

        """
        self.bucket_name = bucket_name
        self.temp_folder = temp_folder
        self.temp_filename = temp_filename
        self.s3_filepath = s3_filepath
        self.prefix = prefix
        self.full_extraction = is_full_extraction

    def unarchive_and_delete_all(self):
        """
        Method to unarchive model files present on S3 locally and re-upload the files back to s3 and delete the original
        files

        Returns:

        """
        if self.s3_filepath is None:
            print("Initiating a process of unarchiving and deleting all the models...")
            for i in range(10):
                sleep(1)
                print(f"Starting in {10 - i} seconds...")

            all_s3_filepaths = self.retrieve_all_s3_filepaths()
            print("Retrieved the following potential filepaths:")
            print(all_s3_filepaths)

            for fp in all_s3_filepaths:
                self.unarchive_and_delete_one(fp)
        else:
            self.unarchive_and_delete_one(self.s3_filepath)

    def retrieve_all_s3_filepaths(self):
        """

        Returns:
            all_s3_filepaths: list: list of all possible filepaths to model tars in the bucket

        """
        # Make sure you provide / in the end
        all_s3_filepaths = list()

        client = boto3.client('s3')
        result = client.list_objects(Bucket=self.bucket_name, Prefix=self.prefix, Delimiter='/')

        for o in result.get('CommonPrefixes'):
            folder_path = o.get('Prefix')
            folder_path_list = folder_path.split("/")
            folder_path_list.append("output")
            folder_path_list.append(self.temp_filename)
            folder_path_s3 = os.path.join(*folder_path_list)
            all_s3_filepaths.append(folder_path_s3)

        return all_s3_filepaths

    @staticmethod
    def reports_and_model(tar):
        for_ret = list()
        for tarinfo in tar:
            if re.match(".+\.\w+", tarinfo.name) or os.path.splitext(tarinfo.name)[0] == "README":
                for_ret.append(tarinfo)
        return for_ret

    @retry(ClientError, tries=5)
    def unarchive_and_delete_one(self,
                                 s3_filepath: str):
        """
        Method to unarchive model files present on S3 locally and re-upload the files back to S3 and delete the original
        files

        Args:
            s3_filepath: str: a filepath to model tar in s3

        Returns:

        """

        temp_tar = os.path.join(self.temp_folder, self.temp_filename)
        s3 = boto3.client('s3')
        os.makedirs(self.temp_folder, exist_ok=True)

        print(f"Starting download of {s3_filepath}...")
        s3.download_file(self.bucket_name, s3_filepath, temp_tar)
        print("Download complete")

        if self.full_extraction:
            print("Extracting all files locally...")
            t = tarfile.open(temp_tar)
            t.extractall(self.temp_folder)
            t.close()
            os.remove(temp_tar)
        else:
            print("Extracting aggregated report and model files locally...")
            t = tarfile.open(temp_tar)
            members = self.reports_and_model(t)
            t.extractall(path=self.temp_folder, members=members)
            # TODO: make sure only the required files are extracted
            t.close()
            os.remove(temp_tar)
        former_path_list = s3_filepath.split("/")
        former_path_list.pop(-1)
        s3_folder = os.path.join(*former_path_list)

        print("Uploading unarchived files back to s3...")
        s3_orm = S3ORM(bucket_name=self.bucket_name)
        s3_orm.upload_files_to_s3(s3_folder=s3_folder,
                                  local_folder_path=self.temp_folder)
        if self.full_extraction:
            print("Deleting archive on S3...")
            s3 = boto3.resource('s3')
            s3.Object(self.bucket_name, s3_filepath).delete()

        print("Deleting local files...")
        shutil.rmtree(self.temp_folder)


class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def get_matching_s3_objects(bucket,
                            aws_access_key_id,
                            aws_secret_access_key,
                            region_name,
                            prefix='',
                            suffix='',
                            max_keys_per_request=1):
    """
    List objects in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    :param max_keys_per_request: number of objects to list down
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      region_name=region_name)
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix
    else:
        kwargs['Prefix'] = str(prefix)

    kwargs['MaxKeys'] = max_keys_per_request

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)

        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def get_matching_s3_keys(bucket,
                         aws_access_key_id,
                         aws_secret_access_key,
                         region_name,
                         prefix='',
                         suffix='',
                         max_keys_per_request=1):
    """
    Generate the keys in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    :param max_keys_per_request: number of objects to list down
    """
    for obj in get_matching_s3_objects(bucket=bucket,
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key,
                                       prefix=prefix,
                                       suffix=suffix,
                                       region_name=region_name,
                                       max_keys_per_request=max_keys_per_request):
        yield obj['Key']


def get_matching_s3_objects(bucket,
                            aws_access_key_id,
                            aws_secret_access_key,
                            region_name,
                            prefix='',
                            suffix='',
                            max_keys_per_request=1000):
    """
    List objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    :param max_keys_per_request: number of objects to list down
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      region_name=region_name)
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix
    else:
        kwargs['Prefix'] = str(prefix)

    kwargs['MaxKeys'] = max_keys_per_request
    # kwargs['StartAfter'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)

        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def get_matching_s3_keys(bucket,
                         aws_access_key_id,
                         aws_secret_access_key,
                         region_name,
                         prefix='',
                         suffix='',
                         max_keys_per_request=1000):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    :param max_keys_per_request: number of objects to list down
    """
    for obj in get_matching_s3_objects(bucket=bucket,
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key,
                                       prefix=prefix,
                                       suffix=suffix,
                                       region_name=region_name,
                                       max_keys_per_request=max_keys_per_request):
        yield obj['Key']


def get_s3_objects_size(bucket,
                        aws_access_key_id,
                        aws_secret_access_key,
                        region_name,
                        prefix='',
                        suffix='',
                        max_keys_per_request=1000):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    :param max_keys_per_request: number of objects to list down
    :returns total_size_in_bytes
    """

    total_size_in_bytes = 0
    for obj in get_matching_s3_objects(bucket=bucket,
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key,
                                       prefix=prefix,
                                       suffix=suffix,
                                       region_name=region_name,
                                       max_keys_per_request=max_keys_per_request):
        size = obj['Size']
        total_size_in_bytes += size
    print("Total size in bytes: {} for {}/{}".format(total_size_in_bytes, bucket, prefix))
    return total_size_in_bytes


def success_file_exists(bucket_or_root_folder,
                        success_file_path,
                        aws_access_key,
                        aws_secret_key,
                        region_name='us-west-2'):
    """

    :param bucket_or_root_folder: Eg: s3a://vh-dp-dev/ or data/vh-dp-dev/
    :param success_file_path:  Eg: data/iris/internal/compacted/2018/10/04/PatientProblem
    :param aws_access_key:
    :param aws_secret_key:
    :param region_name:
    :return: bool
    """

    s3 = boto3.resource('s3',
                        aws_access_key_id=aws_access_key,
                        aws_secret_access_key=aws_secret_key,
                        region_name=region_name)

    try:
        s3.Object(bucket_or_root_folder, success_file_path).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise RuntimeError
    return True


def write_object(bucket_name,
                 prefix,
                 file_name,
                 contents,
                 aws_access_key,
                 aws_secret_key,
                 region_name='us-west-2'):
    """

    :param bucket_name:
    :param prefix:
    :param file_name:
    :param contents: String data
    :param aws_access_key:
    :param aws_secret_key:
    :param region_name:
    :return:
    """

    key = prefix + "/" + file_name
    key = key.replace("//", "/")

    local_path = "/tmp/" + bucket_name + "/" + prefix
    local_file = local_path + "/" + file_name

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    f = open(local_file, "w+")
    f.write(contents)
    f.close()

    s3 = boto3.resource('s3',
                        aws_access_key_id=aws_access_key,
                        aws_secret_access_key=aws_secret_key,
                        region_name=region_name)
    object = s3.Object(bucket_name, key)
    object.put(Body=open(local_file, 'rb'), ServerSideEncryption="AES256")


def get_all_s3_keys(bucket, subdirectory, p_s3_client, file_type=''):
    keys = []
    kwargs = {'Bucket': bucket, 'Prefix': subdirectory}
    while True:
        resp = p_s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.endswith(file_type):
                keys.append(key)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys


def move_files(bucket_name, old_file_path, new_file_path,
               aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    s3_resource = boto3.resource('s3',
                                 aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key,
                                 region_name=region_name)
    s3_resource.Object(bucket_name, new_file_path).copy_from(
        CopySource=bucket_name + "/" + old_file_path, ServerSideEncryption='AES256')
    s3_resource.Object(bucket_name, old_file_path).delete()
    s3_resource.Object(bucket_name, old_file_path + ".metadata").delete()


# -----------------------------------------------------------------------------------
# Function delete_all_folder_files. Clean all files in a directory, so we can recreate
#     the Table Data completely. Use with caution.
# -----------------------------------------------------------------------------------
def delete_all_folder_files(s3_input_bucket, s3_input_path):
    # -----------------------------------------------------
    # -- Indentify the files to delete in the directory (*.*)
    # -----------------------------------------------------
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    try:
        file_name_list = get_all_s3_keys(s3_input_bucket, s3_input_path, s3_client)
    except:
        return ""  # If folder doesnt' exist, just return.
    bucket = s3_resource.Bucket(s3_input_bucket)
    l_counter = 0
    for file_name in file_name_list:
        i_file_name = file_name.replace(s3_input_path, "")  # Removes the path
        response = bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': file_name
                    }
                ]
            }
        )


def delete_metadata_files(s3_input_bucket, s3_input_path):
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    file_name_list = get_all_s3_keys(s3_input_bucket, s3_input_path, s3_client)
    bucket = s3_resource.Bucket(s3_input_bucket)
    for file_name in file_name_list:
        i_file_name = file_name.replace(s3_input_path, "")  # Removes the path
        file_extension = i_file_name[-4:]
        if file_extension != '.csv':
            bucket.delete_objects(
                Delete={
                    'Objects': [
                        {
                            'Key': file_name
                        }
                    ]
                }
            )


if __name__ == '__main__':
    # s3_utils = S3Utils(bucket_name='sagemaker-us-east-1-113379206287', is_full_extraction=True)
    # s3_filepath='Company-English/model_output/TESTING-BERT-FULL-SIZE-yaniv-sagemaker--2020-01-28-10-52-33-844/output/model.tar.gz'
    # s3_filepath='NLP-core/model_output/TESTING-FASTTEXT-FULL-SIZE-viktor-sagem-2020-01-14-12-59-55-120/output/model.tar.gz'
    # s3_utils.unarchive_and_delete_one(s3_filepath)
    # s3_orm = S3ORM(temp_folder='temp2',
    #                debug=True)
    # s3_orm.unarchive_to_s3(
    #     path_from='NLP-core/data_sets/company/english_company/from_company_team/temp/cse-train-small-labeled-filtered.rar',
    #     delete_source=True,
    #     is_train_test_split=True,
    #     train_test_split_ratio=0.9
    # )
    # s3_orm.unarchive_from_s3_locally(path_from="sagemaker/golden_set/biluo_json/json-trit-company-organisation.zip")
    # s3_orm.unarchive_to_s3(path_from="sagemaker/golden_set/biluo_json/json-trit-stanford-company-organisation.zip",
    #                        delete_source=True)

    # s3_filepath = 'NLP-core/model_output/P-F-FULL-viktor-eng-person-tf1-2020-03-23-11-20-40-924/output/model.tar.gz'
    # s3_utils.unarchive_and_delete_one(s3_filepath)
    s3_orm = S3ORM(temp_folder="/home/andriy/Code/PycharmProjects/AWS_utils/resources", bucket_name=BUCKET_NAME)
    s3_orm.download_files_from_s3(s3_folder='NLP-core/data_sets/CTP/DocTypeTrainPart3')
    s3_orm.download_file_from_s3()
    # s3_orm.upload_files_to_s3(s3_folder='NLP-core/data_sets/CTP/DocTypeTrainPart3',
    #                           local_folder_path='/home/ubuntu/Downloads/DocTypeTrainPart3')
    # s3_orm.download_file_from_s3(s3_filepath)
    # s3_utils.unarchive_and_delete_all()
    # print(members)
    # TODO: refactor to several files
    # TODO: remove the unclear parts
    # TODO: add tests
    # TODO: add documentation
    # TODO: improve usage section in README.md

