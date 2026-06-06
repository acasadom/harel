"""FaaS `lambda_action` against a REAL AWS Lambda (LocalStack) in the stack.

The unit tests use a fake boto3 client; this runs the action as an actual Lambda.
`deploy/faas_function.py` (`handler(charge)`) is packaged into a zip by
`deploy/Dockerfile.lambda` (built on the Lambda base so pydantic-core's binary matches
the runtime), and `lambda_action` invokes it through real boto3 — exercising the boto3
invoke transport, the Payload encoding, and `handler(fn)` in a genuine Lambda runtime
(the one transport a fake can't fully cover). Zip packaging because LocalStack
community doesn't support image packaging (Pro only).

Gated on STM_LAMBDA_ENDPOINT + STM_LAMBDA_ZIP (set by the compose `test` service);
skipped otherwise. Setup:

    docker build --platform=linux/amd64 -f deploy/Dockerfile.lambda \
        --target export --output deploy/build .
    docker compose -f deploy/docker-compose.yml up -d localstack
    docker compose -f deploy/docker-compose.yml run --rm test
"""

import os
from pathlib import Path

import pytest

from harel import lambda_action
from harel.spec.states import Event

pytestmark = pytest.mark.stack

_FUNCTION = "harel-charge"


class _Stm:
    """The action proxy the driver would pass — only the surface lambda_action reads."""

    def __init__(self, ctx, key):
        self.execution_ctx = ctx
        self.idempotency_key = key


@pytest.fixture(scope="module")
def lambda_client():
    endpoint = os.environ.get("STM_LAMBDA_ENDPOINT")
    zip_path = os.environ.get("STM_LAMBDA_ZIP")
    if not endpoint or not zip_path:
        pytest.skip("STM_LAMBDA_ENDPOINT / STM_LAMBDA_ZIP not set (not the lambda stack)")
    if not Path(zip_path).exists():
        pytest.skip(f"{zip_path} missing — build it with the Dockerfile.lambda export target")

    import boto3
    from botocore.config import Config

    client = boto3.client(
        "lambda",
        endpoint_url=endpoint,
        region_name=os.environ.get("STM_AWS_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        # generous read timeout: a cold Lambda container can take a while to come up
        config=Config(read_timeout=120, connect_timeout=10, retries={"max_attempts": 0}),
    )
    _ensure_function(client, Path(zip_path).read_bytes())
    return client


def _ensure_function(client, zip_bytes: bytes) -> None:
    """Create the zip-packaged function (idempotent) and wait until it's invokable."""
    from botocore.exceptions import ClientError

    try:
        client.create_function(
            FunctionName=_FUNCTION,
            Runtime="python3.13",
            Handler="faas_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            # must match the zip's binaries: Dockerfile.lambda builds for linux/amd64,
            # so pydantic-core's wheel is x86_64 (the Lambda default, but be explicit)
            Architectures=["x86_64"],
            # LocalStack community doesn't validate IAM; any well-formed arn works
            Role="arn:aws:iam::000000000000:role/lambda-role",
            Timeout=60,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceConflictException":
            raise  # anything other than "already exists" is a real failure
    client.get_waiter("function_active_v2").wait(FunctionName=_FUNCTION)


def test_lambda_action_invokes_a_real_lambda(lambda_client):
    action = lambda_action(_FUNCTION, client=lambda_client)
    stm = _Stm({"n": 1}, key="exe:7:0")
    result = action(stm, Event(kind="Pay"), amount=42)

    # the function ran in a real Lambda runtime, applied the context, and routed
    assert result == "ok"
    assert stm.execution_ctx == {"n": 1, "charged": 42, "key": "exe:7:0"}


def test_lambda_action_routes_the_skip_branch(lambda_client):
    # no amount -> the action returns "skip" (a modelled outcome, not an error)
    action = lambda_action(_FUNCTION, client=lambda_client)
    stm = _Stm({}, key="exe:7:1")
    assert action(stm, Event(kind="Pay")) == "skip"
    assert stm.execution_ctx == {"charged": 0, "key": "exe:7:1"}
