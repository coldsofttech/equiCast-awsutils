import os
from dataclasses import dataclass
from typing import Optional, List

import boto3
from botocore.exceptions import ClientError


@dataclass
class S3:
    bucket_name: str
    region_name: str
    session: Optional[boto3.Session] = None

    def __post_init__(self):
        if not self.session:
            self.session = boto3.Session(region_name=self.region_name)

        self.client = self.session.client("s3", region_name=self.region_name)

    def _download_file(self, s3_key: str, local_path: str) -> bool:
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.client.download_file(self.bucket_name, s3_key, local_path)
            return True
        except ClientError as e:
            print(f"Download failed for {s3_key}: {e}")
            return False

    def _upload_file(self, s3_key: str, file_path: str) -> bool:
        if not s3_key:
            print("Upload failed: missing s3 key")
            return False

        try:
            self.client.upload_file(file_path, self.bucket_name, s3_key)
            return True
        except FileNotFoundError as e:
            print(f"Upload failed for {file_path}: {e}")
            return False
        except ClientError as e:
            print(f"Upload failed for {s3_key}: {e}")
            return False

    def download_files(self, local_dir: str, files: Optional[List[dict]] = None, download_all: bool = True) -> dict:
        """
        :param local_dir: local directory to download files to
        :param files: list of files to download. [{'key': 'file1.txt', 'mandatory': true}]
        :param download_all: if True, download all files in local_dir
        :return: dict
        """
        os.makedirs(local_dir, exist_ok=True)
        missing_mandatory = []
        downloaded_files = []

        if download_all:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    local_path = os.path.join(local_dir, key)
                    if self._download_file(key, local_path):
                        downloaded_files.append(key)

            return {"downloaded": downloaded_files, "missing_mandatory": []}

        if not files:
            print("No files to download")
            return {"downloaded": [], "missing_mandatory": []}

        for file_info in files:
            key = file_info.get("key")
            mandatory = file_info.get("mandatory", False)
            local_path = os.path.join(local_dir, key)

            if self._download_file(key, local_path):
                downloaded_files.append(key)
            elif mandatory:
                missing_mandatory.append(key)

        if missing_mandatory:
            raise FileNotFoundError(f"Mandatory files missing in S3: {missing_mandatory}")

        return {"downloaded": downloaded_files, "missing_mandatory": missing_mandatory}

    def upload_files(self, files: List[dict]) -> dict:
        """
        :param files: list of files to upload. [{'key': 'file1.txt', 'path': '/local/file1.txt'}]
        :return:
        """
        uploaded_files = []
        failed_files = []

        for file_info in files:
            s3_key = file_info.get("key")
            file_path = file_info.get("path")

            if not s3_key or not file_path:
                failed_files.append({"key": s3_key, "path": file_path})
                continue

            if self._upload_file(s3_key, file_path):
                uploaded_files.append(s3_key)
            else:
                failed_files.append({"key": s3_key, "path": file_path})

        return {"uploaded": uploaded_files, "failed": failed_files}
