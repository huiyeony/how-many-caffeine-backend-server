from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI

from rag.promps import CAFFEINE_GUIDE_PROMPT
from rag.tool import search_by_brand, search_by_brand_and_menu, search_by_menu

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
llm_with_tools = llm.bind_tools([search_by_brand, search_by_menu, search_by_brand_and_menu])

_tools_map = {
    "search_by_brand": search_by_brand,
    "search_by_menu": search_by_menu,
    "search_by_brand_and_menu": search_by_brand_and_menu,
}


def build_history(rows: list[dict]) -> list:
    """PostgreSQL chat 테이블 rows → LangChain 메시지 리스트"""
    messages = [SystemMessage(content=CAFFEINE_GUIDE_PROMPT)]
    for row in rows:
        if row["role"] == "user":
            messages.append(HumanMessage(content=row["content"]))
        elif row["role"] == "assistant":
            messages.append(AIMessage(content=row["content"]))
    return messages


async def run_rag(q: str, history: list) -> str:
    """RAG 실행. 답변 텍스트 반환."""
    history.append(HumanMessage(content=q))

    ai_msg = await llm_with_tools.ainvoke(history)

    if ai_msg.tool_calls:
        history.append(ai_msg)
        for tool_call in ai_msg.tool_calls:
            result = await _tools_map[tool_call["name"]].ainvoke(tool_call)
            history.append(ToolMessage(content=str(result), tool_call_id=tool_call["id"]))
        final = await llm.ainvoke(history)
        return final.content

    return ai_msg.content


