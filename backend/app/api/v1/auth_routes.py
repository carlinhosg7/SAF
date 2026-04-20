from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.schemas.auth import LoginRequest
from app.services.auth_service import login

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/login")
def login_user(data: LoginRequest, db: Session = Depends(get_db)):
    user = login(db, data.codigo_usuario, data.senha)

    if not user:
        raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")

    return {
        "id": user.id,
        "nome": user.nome,
        "nivel": user.nivel
    }