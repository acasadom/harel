"""The FaaS function deployed as a real AWS Lambda for the stack test.

`handler(charge)` adapts the ordinary `(stm, event, **inputs)` action to the Lambda
entrypoint contract — AWS calls `lambda_handler(event, context)` where `event` is the
JSON the client sent (the `{context, event, inputs, idempotency_key}` payload), and the
returned dict `{context, result}` becomes the invoke response Payload. The SAME `charge`
would be bound locally with `actions={"charge": charge}`; here it runs serverless.

Built into a Lambda image by `deploy/Dockerfile.lambda` and invoked through
`harel.faas.lambda_action` in `test/integration/test_faas_lambda.py`.
"""

from harel.faas import handler


def charge(stm, event, **inputs):
    """A side-effecting action: records the (fake) charge + the engine's idempotency
    key into context, and returns a routing key a selector would branch on."""
    amount = inputs.get("amount", 0)
    stm.execution_ctx["charged"] = amount
    stm.execution_ctx["key"] = stm.idempotency_key
    return "ok" if amount > 0 else "skip"


# the AWS Lambda entrypoint (the image's CMD points at this)
lambda_handler = handler(charge)
