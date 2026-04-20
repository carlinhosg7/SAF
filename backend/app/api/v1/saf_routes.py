from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.schemas.saf import CriarSAF
from app.services.saf_service import criar_saf

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/criar")
def criar(data: CriarSAF, db: Session = Depends(get_db)):
    return criar_saf(db, data.model_dump())