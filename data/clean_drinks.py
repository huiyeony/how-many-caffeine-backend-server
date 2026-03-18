import pandas as pd
import os
import logging
from datetime import datetime

# 로깅 설정 (엔지니어의 기본)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- [Data Cleansing Configuration] ---
# 브랜드명 정규화 매핑 (Alias -> Canonical)
BRAND_ALIAS_MAP = {
    "컴포즈커피": "컴포즈",
    "매머드익스프레스": "매머드커피",
    "더벤티커피": "더벤티"
}

# 얼음 타입 정규화 매핑
ICE_TYPE_MAP = {
    "h0t": "hot",
    "hOt": "hot",
    "h0T": "hot",
    "icee": "ice",
    "iCe": "ice"
}

def clean_drink_dataset(input_path: str, output_path: str):
    logger.info(f">>> [Process] Starting Data Cleansing Pipeline: {input_path}")
    
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        return

    # 1. 데이터 로드 (E)
    df = pd.read_csv(input_path)
    initial_count = len(df)
    logger.info(f"Initial row count: {initial_count}")

    # 2. 데이터 변환 (T)
    # [T1] 브랜드명 정규화
    df['brand'] = df['brand'].replace(BRAND_ALIAS_MAP)
    
    # [T2] 얼음 타입 정규화 (공백 제거 후 소문자 변환 후 매핑)
    df['ice_type'] = df['ice_type'].str.strip().str.lower()
    df['ice_type'] = df['ice_type'].replace(ICE_TYPE_MAP)
    # 매핑되지 않은 값 중 hot/ice가 아니면 'unknown' 처리
    df.loc[~df['ice_type'].isin(['hot', 'ice']), 'ice_type'] = 'ice' # 기본값 ice

    # [T3] 음료 이름 정규화 (불필요한 공백 제거)
    df['drink_name'] = df['drink_name'].str.strip()

    # [T4] 카페인 함량 정규화
    df['caffeine_amount'] = pd.to_numeric(df['caffeine_amount'], errors='coerce').fillna(0.0)
    
    # [T5] 이상치 제거 (카페인 함량 0 미만 또는 1000 초과)
    invalid_rows = df[(df['caffeine_amount'] < 0) | (df['caffeine_amount'] > 1000)]
    if not invalid_rows.empty:
        logger.warning(f"Removing {len(invalid_rows)} invalid caffeine records.")
        df = df[(df['caffeine_amount'] >= 0) & (df['caffeine_amount'] <= 1000)]

    # [T6] 중복 데이터 제거 (브랜드 + 이름 + 타입 기반)
    # 중복 데이터 중 마지막 데이터를 남김
    df = df.drop_duplicates(subset=['brand', 'drink_name', 'ice_type'], keep='last')
    
    final_count = len(df)
    logger.info(f"Final cleaned row count: {final_count} (Removed: {initial_count - final_count})")

    # [T7] 메타데이터 추가 (추후 추적성 확보)
    df['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 3. 데이터 저장 (L)
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f">>> [Success] Cleaned data saved to: {output_path}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    INPUT_CSV = os.path.join(BASE_DIR, "drinks.csv")
    OUTPUT_CSV = os.path.join(BASE_DIR, "drinks_silver.csv")
    
    clean_drink_dataset(INPUT_CSV, OUTPUT_CSV)
