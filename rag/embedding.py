from typing import List
from core.config import settings
from rag.model import embeddings


async def get_embeddings(texts: List[str]) -> List[List[float]]:
    """여러 문서(텍스트)를 한 번에 벡터로 변환합니다."""
    return await embeddings.aembed_documents(texts)

async def get_query_embedding(query: str) -> List[float]:
    """단일 검색 쿼리를 벡터로 변환합니다."""
    return await embeddings.aembed_query(query)

