import json

import boto3
import psycopg

from core.config import settings
from core.database import get_pool
from rag.embedding import get_embeddings


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
    )


def download_raw_from_s3(s3_key: str) -> list[dict]:
    """S3에서 raw JSON 다운로드"""
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET_NAME, Key=s3_key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def transform(raw: list[dict]) -> list[dict]:
    """
    ELT의 T 단계: raw 원본 → drinks 테이블 스키마로 변환

    - 카페인 값 정제 (None → 0.0, 음수/1000 초과 제거)
    - ice_type 추론 (음료명 기반)
    - 브랜드명 정규화
    """
    BRAND_ALIAS = {
        "테라 커피":      "테라커피",
        "teracoffee":     "테라커피",
        "mammothcoffee":  "매머드커피",
        "megacoffee":     "메가커피",
        "composecoffee":  "컴포즈",
    }

    transformed = []
    for item in raw:
        brand = BRAND_ALIAS.get(item.get("brand", ""), item.get("brand", ""))
        drink_name = (item.get("name") or "").strip()

        if not drink_name:
            continue

        # 카페인 정제 (크롤러 필드명: caffeine_mg)
        try:
            caffeine = float(item.get("caffeine_mg") or 0.0)
        except (ValueError, TypeError):
            caffeine = 0.0

        if caffeine < 0 or caffeine > 1000:
            continue

        # ice_type: 크롤러가 이미 제공한 경우 우선 사용, 없으면 음료명에서 추론
        raw_ice = (item.get("ice_type") or "").upper()
        if raw_ice in ("HOT", "ICE"):
            ice_type = raw_ice.lower()
        else:
            name_lower = drink_name.lower()
            if any(k in name_lower for k in ["아이스", "ice", "cold"]):
                ice_type = "ice"
            elif any(k in name_lower for k in ["핫", "hot", "따뜻"]):
                ice_type = "hot"
            else:
                ice_type = "ice"  # 기본값

        transformed.append({
            "brand": brand,
            "drink_name": drink_name,
            "caffeine_amount": caffeine,
            "ice_type": ice_type,
        })

    return transformed


async def load_to_db(records: list[dict]):
    """변환된 데이터 임베딩 생성 후 PostgreSQL upsert"""
    if not records:
        print(">>> [Loader] 삽입할 데이터가 없습니다.")
        return

    batch_size = 100
    total = len(records)

    async with get_pool().connection() as conn:
        async with conn.cursor() as cur:
            for i in range(0, total, batch_size):
                batch = records[i: i + batch_size]

                texts = [
                    f"브랜드: {r['brand']} 음료명: {r['drink_name']} {r['ice_type']} 카페인: {r['caffeine_amount']}mg"
                    for r in batch
                ]
                vectors = await get_embeddings(texts)

                rows = [
                    (
                        r["brand"],
                        r["drink_name"],
                        r["caffeine_amount"],
                        r["ice_type"],
                        vectors[j],
                    )
                    for j, r in enumerate(batch)
                ]

                await cur.executemany(
                    """
                    INSERT INTO drinks (brand, drink_name, caffeine_amount, ice_type, embedding)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (brand, drink_name, ice_type)
                    DO UPDATE SET
                        caffeine_amount = EXCLUDED.caffeine_amount,
                        embedding = EXCLUDED.embedding;
                    """,
                    rows,
                )

                print(f">>> [Loader] {min(i + batch_size, total)} / {total} upsert 완료")

        await conn.commit()
    print(">>> [Loader] DB 적재 완료")


async def run_loader(s3_key: str):
    """S3 raw → 변환 → PostgreSQL"""
    print(f">>> [Loader] S3 raw 읽기: {s3_key}")
    raw = download_raw_from_s3(s3_key)

    print(f">>> [Loader] 변환 시작 (raw {len(raw)}건)")
    records = transform(raw)
    print(f">>> [Loader] 변환 완료 ({len(records)}건 유효)")

    await load_to_db(records)
