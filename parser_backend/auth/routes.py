"""
backend/auth/routes.py
──────────────────────
POST /auth/register  – register a new user via Supabase Auth
POST /auth/login     – authenticate via Supabase Auth, returns Supabase JWT
"""
from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from auth.utils import register_user, login_user

router = APIRouter()


# ── Schemas ─────────────────────────────────────────────────

class UserCreate(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ── Register ────────────────────────────────────────────────

@router.post("/register", status_code=201)
def register(body: UserCreate):
    if len(body.password) < 8:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Password must be at least 8 characters.",
        )
    try:
        result = register_user(body.email, body.password)
    except ValueError as e:
        err = str(e).lower()
        if "already" in err or "registered" in err or "exists" in err:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="An account with this email already exists.",
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {e}",
        )
    return {
        "message": "Account created successfully. Please check your email to confirm.",
        "user_id": result["user_id"],
    }


# ── Login ───────────────────────────────────────────────────

@router.post("/login", response_model=TokenResponse)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    try:
        result = login_user(form_data.username, form_data.password)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password.",
        )
    return {
        "access_token": result["access_token"],
        "token_type": "bearer",
    }
