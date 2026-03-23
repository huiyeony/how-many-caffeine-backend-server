"""로컬 전용 파이프라인 — S3 없이 크롤링 → 직접 DB 적재"""
import asyncio
from core.database import init_pool, close_pool
from tasks.crawler import BRAND_REGISTRY
from tasks.loader import transform, load_to_db


async def main():
    await init_pool()
    print(">>> [Local Pipeline] 시작")

    failed = []
    for brand, crawler_cls in BRAND_REGISTRY.items():
        print(f">>> [Local Pipeline] {brand} 크롤링 중...")
        try:
            raw = crawler_cls().crawl()
            if not raw:
                print(f">>> [Local Pipeline] {brand} 수집 결과 없음, 건너뜀")
                continue
            print(f">>> [Local Pipeline] {brand} {len(raw)}개 수집, DB 적재 중...")
            records = transform(raw)
            await load_to_db(records)
            print(f">>> [Local Pipeline] {brand} 완료")
        except Exception as e:
            print(f">>> [Local Pipeline] {brand} 실패 (건너뜀): {e}")
            failed.append(brand)

    if failed:
        print(f">>> [Local Pipeline] 완료 (실패 브랜드: {', '.join(failed)})")
    else:
        print(">>> [Local Pipeline] 전체 완료")
    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
