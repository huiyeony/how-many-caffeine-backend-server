from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """애플리케이션 설정 클래스

    ..env 파일 또는 환경 변수에서 자동으로 값을 로드합니다.
    타입 검증 및 필수 값 체크를 자동으로 수행합니다.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,  # 대소문자 구분
        extra="ignore",  # 추가 환경 변수 무시
    )
    #OPENAI
    OPENAI_API_KEY: str
    
    # Database
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str

    # AWS (미사용 - S3 연동 시 필수값으로 변경)
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None
    AWS_S3_BUCKET_NAME: str | None = None
    AWS_REGION: str = "ap-northeast-2"

    # JWT (미사용 - 인증 기능 구현 시 필수값으로 변경)
    JWT_SECRET_KEY: str | None = None
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_HOURS: int = 24
    REFRESH_TOKEN_EXPIRE_DAY: int = 7

    # Google OAuth (미사용 - OAuth 구현 시 필수값으로 변경)
    GOOGLE_CLIENT_ID: str | None = None
    GOOGLE_CLIENT_SECRET: str | None = None
    GOOGLE_REDIRECT_URI: str | None = None

    # Ollama (미사용 - 로컬 모델 사용 시 필수값으로 변경)
    OLLAMA_BASE_URL: str | None = None

    # Frontend
    FRONTEND_URL: str = "http://localhost:3000"

    # Redis (미사용 - 세션/캐시 연동 시 필수값으로 변경)
    REDIS_URL: str | None = None

    # LangGraph
    LANGSMITH_API_KEY: str
    LANGSMITH_TRACING: bool
    LANGSMITH_ENDPOINT: str = "https://api.smith.langchain.com"
    LANGSMITH_PROJECT: str

    # Environment
    ENVIRONMENT: Literal["development", "production"] = "development"

    @property
    def database_url(self) -> str:
        """PostgreSQL 연결 URL 생성"""
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"


settings = Settings()