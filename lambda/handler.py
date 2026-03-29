import asyncio
from tasks.pipeline import run_pipeline


def handler(event, context):
    asyncio.run(run_pipeline())
    return {"statusCode": 200, "body": "pipeline completed"}
