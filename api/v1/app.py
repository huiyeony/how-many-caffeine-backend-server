# from data.seed import seed_initial_data
from data.seed import seed_initial_data
from rag.promps import CAFFEINE_GUIDE_PROMPT
from rag.tool import search_caffeine_by_brands
from core.database import init_db, init_pool, close_pool, get_pool
from core.config import settings
from tasks.pipeline import run_pipeline
from core.history import ensure_table_exists, load_history, save_history
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()


scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await init_pool()

    # DB가 비어 있으면 초기 로컬 CSV 시딩
    async with get_pool().connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM drinks")
            count = (await cur.fetchone())[0]

    if count == 0:
        await seed_initial_data()
    else:
        print(f">>> [API] Skipping seed: {count} drinks already in DB.")

    # DynamoDB 히스토리 테이블 확인 및 생성
    ensure_table_exists()

    # ELT 파이프라인 스케줄 등록 (매주 월요일 새벽 3시)
    scheduler.add_job(run_pipeline, CronTrigger(day_of_week="mon", hour=3, minute=0))
    scheduler.start()
    print(">>> [API] Server started")

    yield

    scheduler.shutdown()
    await close_pool()
    print(">>> [API] Server shutdown, Scheduler stopped.")
    
app = FastAPI(lifespan=lifespan)

# 1. CORS 설정 (프론트엔드 접속 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. openai API 연결 및 도구 바인딩
# OpenAI 모델 객체 초기화 (gpt-4o-mini 또는 gpt-4o 지정 가능)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
llm_with_tools = llm.bind_tools([search_caffeine_by_brands])

@app.get("/ask")
async def ask_caffeine(q: str = Query(...), session_id: str = Query(default="default")):

    # STEP 1: DynamoDB에서 세션 히스토리 로드
    history = load_history(session_id)
    if not history:
        history.append(SystemMessage(content=CAFFEINE_GUIDE_PROMPT))

    history.append(HumanMessage(content=q))

    # STEP 2: 첫 번째 요청 (키워드 추출 및 도구 호출)
    ai_msg = await llm_with_tools.ainvoke(history)

    # STEP 3: DB 검색 수행
    if ai_msg.tool_calls:
        history.append(ai_msg)

        for tool_call in ai_msg.tool_calls:
            search_result = await search_caffeine_by_brands.ainvoke(tool_call)
            history.append(ToolMessage(
                content=str(search_result),
                tool_call_id=tool_call["id"]
            ))

        # STEP 4: 검색 결과 포함한 히스토리로 최종 답변 생성
        final_response = await llm.ainvoke(history)
        history.append(final_response)
        save_history(session_id, history)
        return {"answer": final_response.content}

    # 도구 호출 없이 직접 답변 가능한 경우 (예: "안녕")
    history.append(ai_msg)
    save_history(session_id, history)
    return {"answer": ai_msg.content}