from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core.config import settings
from core.database import close_pool, init_db, init_pool, get_pool
from api.v1.routes.auth import router as auth_router
from api.v1.routes.chatspace import router as chatspace_router
from api.v1.routes.chat import router as chat_router

load_dotenv()


scheduler = AsyncIOScheduler()


async def delete_old_guests():
    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM users WHERE provider = 'guest' AND created_at < NOW() - INTERVAL '24 hours'"
            )
    print(">>> [Scheduler] Old guest users deleted")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await init_pool()

    # 게스트 유저 삭제 스케줄 등록 (매시간, 24시간 경과 기준)
    scheduler.add_job(delete_old_guests, CronTrigger(hour="*"))
    scheduler.start()
    print(">>> [API] Server started")

    yield

    scheduler.shutdown()
    await close_pool()
    print(">>> [API] Server shutdown, Scheduler stopped.")

app = FastAPI(lifespan=lifespan)

# CORS 설정 (프론트엔드 접속 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(auth_router)
app.include_router(chatspace_router)
app.include_router(chat_router)


