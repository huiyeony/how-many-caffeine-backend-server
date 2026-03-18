import os
import psycopg

def get_db_connection():
    """DB 접속 정보 공통 함수"""
    return psycopg.connect(
        host=os.getenv('POSTGRES_HOST', 'db'),
        dbname=os.getenv('POSTGRES_DB', 'caffeine_db'),
        user=os.getenv('POSTGRES_USER', 'myuser'),
        password=os.getenv('POSTGRES_PASSWORD', 'mypassword'),
        port=os.getenv('POSTGRES_PORT', '5432'),
    )

def init_db():
    """기존 데이터 유지하며 신규 테이블 및 인덱스 생성"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 1. 확장 기능 활성화
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
            # 2. 신규 테이블 생성 (IF NOT EXISTS로 기존 데이터 보호)
            cur.execute("""
                DROP TABLE drinks;
                CREATE EXTENSION IF NOT EXISTS vector;    -- pgvector (벡터 검색용) [cite: 11]
                CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- pg_trgm (오타 교정 및 유사 검색용) [cite: 41]

                CREATE TABLE IF NOT EXISTS drinks (
                    id SERIAL PRIMARY KEY,
                    drink_name TEXT NOT NULL,
                    brand TEXT NOT NULL,
                    caffeine_amount NUMERIC,
                    ice_type TEXT,
                    embedding VECTOR(1536), 
                    UNIQUE(brand, drink_name, ice_type)
                );   
            """)
            
            # 3. 인덱스 생성은 CREATE 바깥에서 생성하는것이 표준
            # 음료명과 브랜드에 인덱스 생성 
            cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_drink_name ON drinks(drink_name);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_brand ON drinks(brand);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_drink_name_trgm ON drinks USING gin (drink_name gin_trgm_ops);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_drinks_embedding ON drinks USING hnsw (embedding vector_cosine_ops);")
            
            conn.commit()
            print(">>> [Core] Database initialized and indexes verified.")