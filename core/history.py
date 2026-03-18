"""
DynamoDB 기반 대화 히스토리 관리

테이블 스키마:
  PK: session_id (String)
  messages: String (JSON 직렬화된 메시지 목록)
  expires_at: Number (TTL, 7일 자동 만료)
"""

import json
import time

import boto3
from botocore.exceptions import ClientError
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from core.config import settings

TABLE_NAME = "howmanycaffeine-chat-history"
TTL_SECONDS = 60 * 60 * 24 * 7  # 7일


def _get_client():
    return boto3.client(
        "dynamodb",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
    )


def ensure_table_exists():
    """테이블이 없으면 자동 생성 + TTL 활성화"""
    client = _get_client()
    try:
        client.describe_table(TableName=TABLE_NAME)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "session_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        client.get_waiter("table_exists").wait(TableName=TABLE_NAME)
        client.update_time_to_live(
            TableName=TABLE_NAME,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
        )
        print(f">>> [History] DynamoDB 테이블 생성 완료: {TABLE_NAME}")


# ── 메시지 직렬화 / 역직렬화 ─────────────────────────────────

def _serialize(msg: BaseMessage) -> dict:
    data = {"type": msg.type, "content": msg.content}
    if isinstance(msg, ToolMessage):
        data["tool_call_id"] = msg.tool_call_id
    if isinstance(msg, AIMessage) and msg.tool_calls:
        data["tool_calls"] = msg.tool_calls
    return data


def _deserialize(data: dict) -> BaseMessage:
    t = data["type"]
    content = data.get("content", "")
    if t == "human":
        return HumanMessage(content=content)
    if t == "ai":
        return AIMessage(content=content, tool_calls=data.get("tool_calls", []))
    if t == "system":
        return SystemMessage(content=content)
    if t == "tool":
        return ToolMessage(content=content, tool_call_id=data["tool_call_id"])
    raise ValueError(f"알 수 없는 메시지 타입: {t}")


# ── 외부 인터페이스 ──────────────────────────────────────────

def load_history(session_id: str) -> list[BaseMessage]:
    """DynamoDB에서 세션 히스토리 로드"""
    client = _get_client()
    resp = client.get_item(
        TableName=TABLE_NAME,
        Key={"session_id": {"S": session_id}},
    )
    item = resp.get("Item")
    if not item:
        return []

    try:
        raw = json.loads(item["messages"]["S"])
        return [_deserialize(m) for m in raw]
    except Exception:
        return []


def save_history(session_id: str, messages: list[BaseMessage]):
    """세션 히스토리를 DynamoDB에 저장 (TTL 갱신)"""
    client = _get_client()
    client.put_item(
        TableName=TABLE_NAME,
        Item={
            "session_id": {"S": session_id},
            "messages": {"S": json.dumps([_serialize(m) for m in messages], ensure_ascii=False)},
            "expires_at": {"N": str(int(time.time()) + TTL_SECONDS)},
        },
    )
