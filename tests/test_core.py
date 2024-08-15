"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import os
import typing as t

from singer_sdk.helpers._compat import importlib_resources
from singer_sdk.testing import get_target_test_class
from singer_sdk.testing.suites import TestSuite
from singer_sdk.testing.templates import TargetFileTestTemplate

import tests.streams as test_streams
from target_redshift.target import TargetRedshift

SAMPLE_CONFIG: dict[str, t.Any] = {
    "host": os.getenv("TARGET_REDSHIFT_HOST"),
    "port": os.getenv("TARGET_REDSHIFT_PORT"),
    "user": os.getenv("TARGET_REDSHIFT_USER"),
    "password": os.getenv("TARGET_REDSHIFT_PASSWORD"),
    "dbname": os.getenv("TARGET_REDSHIFT_DBNAME"),
    "aws_redshift_copy_role_arn": os.getenv("TARGET_REDSHIFT_AWS_REDSHIFT_COPY_ROLE_ARN"),
    "s3_bucket": os.getenv("TARGET_REDSHIFT_S3_BUCKET"),
    "s3_region": os.getenv("TARGET_REDSHIFT_S3_REGION"),
    "s3_key_prefix": os.getenv("TARGET_REDSHIFT_S3_KEY_PREFIX"),
    "default_target_schema": os.getenv("TARGET_REDSHIFT_DEFAULT_TARGET_SCHEMA"),
    "aws_credentials": {
        "aws_access_key_id": os.getenv("TARGET_REDSHIFT_AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("TARGET_REDSHIFT_AWS_SECRET_ACCESS_KEY"),
        "aws_session_token": os.getenv("TARGET_REDSHIFT_AWS_SESSION_TOKEN"),
        "aws_region_name": os.getenv("TARGET_REDSHIFT_AWS_REGION_NAME"),
    },
}


class TargetBaseTest(TargetFileTestTemplate):
    """Test Target stream with nested dates."""

    name = "base_test"

    @property
    def singer_filepath(self):
        return importlib_resources.files(test_streams) / "base_test.singer"


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetRedshift,
    config=SAMPLE_CONFIG,
    include_target_tests=False,
    custom_suites=[
        TestSuite(
            kind="target",
            tests=[TargetBaseTest],
        ),
    ],
)


class TestTargetRedshift(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    pass
