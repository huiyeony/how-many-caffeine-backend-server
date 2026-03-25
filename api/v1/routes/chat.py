from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from psycopg.rows import dict_row

from core.auth import get_current_user
from core.database import get_pool
from rag.pipeline import build_history, run_rag

router = APIRouter(prefix="/chatspaces")


@router.get("/{chatspace_id}/chats")
async def get_chats(chatspace_id: str, user_id: str = Depends(get_current_user)):
    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT 1 FROM chatspace WHERE chatspace_id = %s AND user_id = %s",
                (chatspace_id, user_id),
            )
            if not await cur.fetchone():
                raise HTTPException(403, "접근 권한이 없습니다")

            await cur.execute(
                "SELECT role, content, created_at FROM chat "
                "WHERE chatspace_id = %s ORDER BY created_at ASC",
                (chatspace_id,),
            )
            return await cur.fetchall()


class SendMessageRequest(BaseModel):
    content: str


@router.post("/{chatspace_id}/chats")
async def send_message(
    chatspace_id: str,
    body: SendMessageRequest,
    user_id: str = Depends(get_current_user),
):
    pool = get_pool()

    # 1. 소유권 확인 + 히스토리 로드
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT 1 FROM chatspace WHERE chatspace_id = %s AND user_id = %s",
                (chatspace_id, user_id),
            )
            if not await cur.fetchone():
                raise HTTPException(403, "접근 권한이 없습니다")

            await cur.execute(
                "SELECT role, content FROM chat WHERE chatspace_id = %s ORDER BY created_at ASC",
                (chatspace_id,),
            )
            rows = await cur.fetchall()

    # 2. RAG 응답 생성
    history = build_history(rows)
    answer = await run_rag(body.content, history)

    # 3. 유저 메시지 + 어시스턴트 응답 한 트랜잭션으로 저장
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO chat (chatspace_id, role, content) VALUES (%s, 'user', %s)",
                (chatspace_id, body.content),
            )
            await cur.execute(
                "INSERT INTO chat (chatspace_id, role, content) VALUES (%s, 'assistant', %s)",
                (chatspace_id, answer),
            )

    return {"answer": answer}
