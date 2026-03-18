from tasks.crawler import run_crawler
from tasks.loader import run_loader


async def run_pipeline():
    """
    ELT 파이프라인
    1. Extract + Load → 테라커피 크롤링 → S3 raw 저장
    2. Transform + Load → S3 raw 읽기 → 변환 → PostgreSQL upsert
    """
    print(">>> [Pipeline] ELT 파이프라인 시작")

    # E → L : 크롤링 후 S3 raw 저장
    s3_key = await run_crawler()

    # T → L : S3 raw 읽어서 변환 후 DB 적재
    await run_loader(s3_key)

    print(">>> [Pipeline] ELT 파이프라인 완료")
