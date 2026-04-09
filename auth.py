"""
Authentication helpers for the API.
Supports two auth modes:
  1. Bearer token (JWT) -- used by the dashboard
  2. X-API-Key header   -- used by the local agent
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Header, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext

from models import get_db

SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(user_id: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    return jwt.encode({"sub": user_id, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except JWTError:
        return None


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    x_api_key: Optional[str] = Header(None),
):
    """Resolve the calling user from JWT bearer token or X-API-Key header."""

    # Try API key first (agent auth)
    if x_api_key:
        conn = get_db()
        row = conn.execute("SELECT id FROM users WHERE api_key = ?", (x_api_key,)).fetchone()
        conn.close()
        if row:
            return dict(row)
        raise HTTPException(status_code=401, detail="Invalid API key")

    # Try JWT bearer (dashboard auth)
    if credentials:
        user_id = decode_token(credentials.credentials)
        if user_id:
            conn = get_db()
            row = conn.execute("SELECT id, email FROM users WHERE id = ?", (user_id,)).fetchone()
            conn.close()
            if row:
                return dict(row)
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    raise HTTPException(status_code=401, detail="Authentication required")
