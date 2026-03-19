import logging
from tasks.crawler import run_crawler, BRAND_REGISTRY
from tasks.loader import run_loader

logger = logging.getLogger(__name__)


async def run_pipeline():
    """
    ELT 파이프라인 — BRAND_REGISTRY에 등록된 모든 브랜드 처리
    1. Extract + Load → 크롤링 → S3 raw 저장
    2. Transform + Load → S3 raw 읽기 → 변환 → PostgreSQL upsert
    브랜드별 실패는 로그로 남기고 나머지 브랜드는 계속 처리
    """
    print(">>> [Pipeline] ELT 파이프라인 시작")

    failed = []
    for brand in BRAND_REGISTRY:
        print(f">>> [Pipeline] {brand} 처리 시작")
        try:
            s3_key = await run_crawler(brand)
            await run_loader(s3_key)
            print(f">>> [Pipeline] {brand} 처리 완료")
        except Exception as e:
            logger.error(f">>> [Pipeline] {brand} 처리 실패: {e}")
            failed.append(brand)

    if failed:
        print(f">>> [Pipeline] 완료 (실패 브랜드: {', '.join(failed)})")
    else:
        print(">>> [Pipeline] ELT 파이프라인 완료")
