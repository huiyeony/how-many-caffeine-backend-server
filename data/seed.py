import asyncio
import csv
from core.database import get_db_connection #
from rag.embedding import get_embeddings #
from dotenv import load_dotenv
async def seed_initial_data(csv_path: str = "data/drinks_silver.csv"):
    load_dotenv()
    with get_db_connection() as conn: 
        with conn.cursor() as cur:
            # # 1. 데이터 존재 여부 확인
            # cur.execute("SELECT EXISTS (SELECT 1 FROM drinks LIMIT 1);") #
            # if cur.fetchone()[0]:
            #     print(">>> [Seed] 이미 데이터가 존재합니다. 삽입을 건너뜁니다.")
            #     return

            # 2. CSV 로드 (DictReader 활용)
            raw_data = []
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    raw_data = list(reader)
            except FileNotFoundError:
                print(f">>> [Seed] {csv_path} 파일을 찾을 수 없습니다.")
                return

            if not raw_data:
                print(">>> [Seed] 데이터가 없습니다.")
                return

            print(f">>> [Seed] {len(raw_data)}건의 데이터를 처리합니다.")

            # 3. 청크(Chunk) 단위 처리 (메모리 및 속도 최적화)
            # 대량의 데이터를 한 번에 임베딩하면 API 타임아웃이나 메모리 부족이 발생할 수 있습니다.
            batch_size = 100 
            for i in range(0, len(raw_data), batch_size):
                chunk = raw_data[i : i + batch_size]
                
                # 임베딩 생성용 텍스트 (ice_type은 현재 스키마에 없으므로 확인 필요)
                # 임베딩 텍스트에 맥락 추가
                texts = [f'''
                         * **브랜드** :{item['brand']} 
                         * **음료명** : {item['drink_name']} {item['ice_type']}
                         * **카페인** :{item['caffeine_amount']}mg
                         ''' 
                         for item in chunk]
                embeddings = await get_embeddings(texts) #

                insert_query = """
                    INSERT INTO drinks (brand, drink_name, caffeine_amount, ice_type, embedding)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (brand, drink_name, ice_type) 
                    DO UPDATE SET
                    caffeine_amount = EXCLUDED.caffeine_amount,
                    embedding = EXCLUDED.embedding;
                """ #

                # 쿼리에 들어갈 튜플 리스트 생성
                rows_to_insert = [
                    (
                        item['drink_name'], 
                        item['brand'], 
                        item.get('caffeine_amount'), 
                        item.get('ice_type'), 
                        embeddings[j]
                    )
                    for j, item in enumerate(chunk)
                ]

                cur.executemany(insert_query, rows_to_insert)
                print(f">>> [Seed] {i + len(chunk)} / {len(raw_data)} 처리 중...")

            conn.commit()
            print(">>> [Seed] 모든 데이터 삽입이 완료되었습니다.")

if __name__ == "__main__":
    asyncio.run(seed_initial_data())