from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

from core.config import settings

pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto",
    bcrypt__truncate_error=False
)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password, hashed)


def create_access_token(user_id: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=settings.JWT_EXPIRE_HOURS)
    payload = {"sub": user_id, "exp": expire}
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def verify_token(token: str) -> str | None:
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        return payload["sub"]
    except JWTError:
        return None


async def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    user_id = verify_token(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")
    return user_id
