import os
import tempfile

import boto3
import pytest
from moto import mock_aws

from equicast_awsutils import S3


@pytest.fixture
def s3_bucket():
    with mock_aws():
        region = "eu-west-1"
        bucket_name = "my-test-bucket"
        s3 = boto3.client("s3", region_name=region)
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )

        s3.put_object(Bucket=bucket_name, Key="file1.txt", Body=b"hello1")
        s3.put_object(Bucket=bucket_name, Key="file2.txt", Body=b"hello2")
        s3.put_object(Bucket=bucket_name, Key="file3.txt", Body=b"hello3")

        yield bucket_name, region


def _read_file(path):
    with open(path, "rb") as f:
        return f.read()


def test_download_all_files(s3_bucket):
    bucket, region = s3_bucket
    s3_client = S3(bucket, region)

    with tempfile.TemporaryDirectory() as tmpdir:
        result = s3_client.download_files(local_dir=tmpdir, download_all=True)

        assert set(result["downloaded"]) == {"file1.txt", "file2.txt", "file3.txt"}
        assert _read_file(os.path.join(tmpdir, "file1.txt")) == b"hello1"


def test_download_specific_file(s3_bucket):
    bucket, region = s3_bucket
    s3_client = S3(bucket, region)

    with tempfile.TemporaryDirectory() as tmpdir:
        result = s3_client.download_files(
            local_dir=tmpdir,
            download_all=False,
            files=[{"key": "file2.txt", "mandatory": True}],
        )

        assert result["downloaded"] == ["file2.txt"]
        assert _read_file(os.path.join(tmpdir, "file2.txt")) == b"hello2"


def test_download_all_mandatory_success(s3_bucket):
    bucket, region = s3_bucket
    s3_client = S3(bucket, region)

    files = [
        {"key": "file1.txt", "mandatory": True},
        {"key": "file2.txt", "mandatory": True},
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        result = s3_client.download_files(local_dir=tmpdir, download_all=False, files=files)

        assert set(result["downloaded"]) == {"file1.txt", "file2.txt"}
        assert result["missing_mandatory"] == []


def test_download_all_non_mandatory_with_missing(s3_bucket):
    bucket, region = s3_bucket
    s3_client = S3(bucket, region)

    files = [
        {"key": "file-does-not-exist.txt", "mandatory": False},
        {"key": "file1.txt", "mandatory": False},
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        result = s3_client.download_files(local_dir=tmpdir, download_all=False, files=files)

        assert result["downloaded"] == ["file1.txt"]
        assert result["missing_mandatory"] == []


def test_download_mixed_mandatory_and_optional_with_missing(s3_bucket):
    bucket, region = s3_bucket
    s3_client = S3(bucket, region)

    files = [
        {"key": "file1.txt", "mandatory": True},
        {"key": "file-does-not-exist.txt", "mandatory": True},
        {"key": "file2.txt", "mandatory": False},
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        with pytest.raises(FileNotFoundError) as exc:
            s3_client.download_files(local_dir=tmpdir, download_all=False, files=files)

        assert "file-does-not-exist.txt" in str(exc.value)


def test_upload_single_file(s3_bucket):
    bucket, region = s3_bucket
    s3_client = S3(bucket, region)

    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        tmpfile.write(b"hello-upload")
        tmpfile_path = tmpfile.name

    assert s3_client._upload_file("single.txt", tmpfile_path) is True

    s3 = boto3.client("s3", region_name=region)
    resp = s3.get_object(Bucket=bucket, Key="single.txt")
    assert resp["Body"].read() == b"hello-upload"

    os.remove(tmpfile_path)


def test_upload_multiple_files_success(s3_bucket):
    bucket, region = s3_bucket
    s3_client = S3(bucket, region)

    tmp_files = []
    for content in [b"data1", b"data2"]:
        f = tempfile.NamedTemporaryFile(delete=False)
        f.write(content)
        f.close()
        tmp_files.append(f.name)

    files = [
        {"key": "file1.txt", "path": tmp_files[0]},
        {"key": "file2.txt", "path": tmp_files[1]},
    ]

    result = s3_client.upload_files(files)

    assert set(result["uploaded"]) == {"file1.txt", "file2.txt"}
    assert result["failed"] == []

    s3 = boto3.client("s3", region_name=region)
    for k, expected in zip(["file1.txt", "file2.txt"], [b"data1", b"data2"]):
        resp = s3.get_object(Bucket=bucket, Key=k)
        assert resp["Body"].read() == expected

    for f in tmp_files:
        os.remove(f)


def test_upload_mixed_success_failure(s3_bucket):
    bucket, region = s3_bucket
    s3_client = S3(bucket, region)

    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        tmpfile.write(b"ok")
        tmpfile_path = tmpfile.name

    files = [
        {"key": "good.txt", "path": tmpfile_path},
        {"key": "missing.txt", "path": "/does/not/exist"},
        {"key": None, "path": tmpfile_path},
    ]

    result = s3_client.upload_files(files)

    assert "good.txt" in result["uploaded"]
    assert any(f["key"] == "missing.txt" for f in result["failed"])
    assert any(f["key"] is None for f in result["failed"])

    os.remove(tmpfile_path)


def test_upload_with_key_patterns(s3_bucket):
    bucket, region = s3_bucket
    s3_client = S3(bucket, region)

    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        tmpfile.write(b"parquet-data")
        tmpfile_path = tmpfile.name

    files = [
        {"key": "fxpairs=EURUSD/file.parquet", "path": tmpfile_path},
        {"key": "ticker=AAPL/stock_price.parquet", "path": tmpfile_path},
    ]

    result = s3_client.upload_files(files)

    assert set(result["uploaded"]) == {
        "fxpairs=EURUSD/file.parquet",
        "ticker=AAPL/stock_price.parquet",
    }
    assert result["failed"] == []

    s3 = boto3.client("s3", region_name=region)
    for k in result["uploaded"]:
        resp = s3.get_object(Bucket=bucket, Key=k)
        assert resp["Body"].read() == b"parquet-data"

    os.remove(tmpfile_path)
