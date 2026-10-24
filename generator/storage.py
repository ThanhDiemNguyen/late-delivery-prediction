"""S3 access for the generator: a client for SeaweedFS with the .env keys.

The keys are never printed. The endpoint defaults to the core profile's http://127.0.0.1:8333 and
can be changed with S3_ENDPOINT.
"""

from __future__ import annotations

import boto3
from botocore.client import BaseClient

from generator.db import read_env


def s3_client() -> BaseClient:
    """Return a boto3 S3 client for SeaweedFS, authenticated with S3_ACCESS_KEY and S3_SECRET_KEY."""
    env = read_env(prefixes=("S3_",))
    missing = [k for k in ("S3_ACCESS_KEY", "S3_SECRET_KEY") if not env.get(k)]
    if missing:
        raise RuntimeError(f"set {', '.join(missing)} in .env (copy .env.example)")
    return boto3.client(
        "s3",
        endpoint_url=env.get("S3_ENDPOINT", "http://127.0.0.1:8333"),
        region_name="us-east-1",
        aws_access_key_id=env["S3_ACCESS_KEY"],
        aws_secret_access_key=env["S3_SECRET_KEY"],
    )
