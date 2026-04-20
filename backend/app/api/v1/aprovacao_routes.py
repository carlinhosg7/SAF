from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db.session import SessionLocal
from app.schemas.aprovacao import AprovarRequest
from app.services.aprovacao_service import aprovar

router = APIRouter(prefix="/aprovacao", tags=["Aprovação"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ================================
# APROVAR SAF
# ================================
@router.post("/aprovar")
def aprovar_saf(data: AprovarRequest, db: Session = Depends(get_db)):
    return aprovar(db, data.model_dump())


# ================================
# LISTAR SAFS PENDENTES
# ================================
@router.get("/listar")
def listar_safs(db: Session = Depends(get_db)):
    query = text("""
        SELECT 
            s.id,
            s.codigo_saf,
            s.tipo_solicitacao,
            s.status_atual,
            a.ordem_nivel,
            a.status_aprovacao
        FROM saf_solicitacoes s
        JOIN saf_aprovacoes a
            ON s.id = a.solicitacao_id
        WHERE a.status_aprovacao = 'PENDENTE'
        ORDER BY s.id, a.ordem_nivel
    """)

    result = db.execute(query).mappings().all()
    return result