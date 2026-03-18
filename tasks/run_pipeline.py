"""ELT 파이프라인 단독 실행 스크립트"""
import asyncio
from core.database import init_pool, close_pool
from tasks.pipeline import run_pipeline


async def main():
    await init_pool()
    await run_pipeline()
    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
