from typing import Optional
import psycopg
# from rag.embedding import embeddings
from langsmith import traceable
from core.config import settings
from core.database import get_pool
from dotenv import load_dotenv

from rag.embedding import get_query_embedding



load_dotenv()


@traceable()
async def search_drinks_hybrid(
    query_text: str,
    fetch_k: int = 10,
    brands: Optional[list[str]] = None
) -> list[any]:
    """
    RRF 하이브리드 검색 함수입니다.
    """
    # 1. 질문 임베딩 생성 (동기)
    query_vector = await get_query_embedding(query_text)
    
    # 2. 하이브리드 검색 SQL (psycopg3 파라미터 형식 적용)
    # psycopg는 %(name)s 형식을 사용합니다.
    sql_query = """
        WITH semantic_search AS (
            SELECT id, drink_name, brand, caffeine_amount, embedding, ice_type, 
                ROW_NUMBER() OVER (ORDER BY embedding <=> %(embedding)s::vector) as rank
            FROM drinks
            WHERE (%(brand)s::text[] IS NULL OR brand = ANY(%(brand)s::text[]))
            ORDER BY embedding <=> %(embedding)s::vector
            LIMIT %(fetch_k)s
        ),
        keyword_search AS (
            SELECT id, drink_name, brand, caffeine_amount, embedding, ice_type, 
                ROW_NUMBER() OVER (ORDER BY similarity(drink_name, %(query)s) DESC) as rank
            FROM drinks
            WHERE (%(brand)s::text[] IS NULL OR brand = ANY(%(brand)s::text[]))
            AND drink_name %% %(query)s
            ORDER BY similarity(drink_name, %(query)s) DESC
            LIMIT %(fetch_k)s
        )
        SELECT 
            COALESCE(s.id, k.id) as id,
            COALESCE(s.drink_name, k.drink_name) as drink_name,
            COALESCE(s.brand, k.brand) as brand,
            COALESCE(s.caffeine_amount, k.caffeine_amount) as caffeine_amount,
            COALESCE(s.embedding, k.embedding) as embedding,
            COALESCE(s.ice_type, k.ice_type, 'ice') as ice_type,
            (COALESCE(1.0 / (s.rank + 100), 0.0) + COALESCE(1.0 / (k.rank + 60), 0.0)) AS rrf_score
        FROM semantic_search s
        FULL OUTER JOIN keyword_search k ON s.id = k.id
        ORDER BY rrf_score DESC
        LIMIT %(fetch_k)s;
    """

    # 벡터 데이터는 '[0.1, 0.2, ...]' 형태의 문자열로 전달해야 합니다.
    params = {
        "embedding": str(query_vector),
        "query": query_text,
        "fetch_k": fetch_k,
        "brand": brands if brands else None
    }

    candidates = []
    
    # 3. psycopg.connect를 사용하여 DB 실행    
    async with get_pool().connection() as conn:
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(sql_query, params)
            candidates = await cur.fetchall()

    if not candidates:
        return []


    # 5. 최종 결과 반환
    return candidates
