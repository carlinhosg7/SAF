from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db.session import SessionLocal

router = APIRouter(prefix="/execucao", tags=["Execução"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/listar")
def listar_execucoes(db: Session = Depends(get_db)):
    query = text("""
        SELECT
            e.id,
            e.solicitacao_id,
            s.codigo_saf,
            s.tipo_solicitacao,
            e.atendente_responsavel_id,
            e.status_execucao,
            e.data_inicio,
            e.data_fim,
            e.observacao_execucao
        FROM saf_execucoes e
        JOIN saf_solicitacoes s
            ON s.id = e.solicitacao_id
        ORDER BY e.id DESC
    """)

    result = db.execute(query).mappings().all()
    return result


@router.post("/iniciar/{solicitacao_id}")
def iniciar_execucao(solicitacao_id: int, db: Session = Depends(get_db)):
    query = text("""
        UPDATE saf_execucoes
        SET
            status_execucao = 'EM_ANDAMENTO',
            data_inicio = CURRENT_TIMESTAMP,
            atendente_responsavel_id = COALESCE(atendente_responsavel_id, 1),
            observacao_execucao = COALESCE(observacao_execucao, 'Execução iniciada')
        WHERE solicitacao_id = :solicitacao_id
    """)

    db.execute(query, {"solicitacao_id": solicitacao_id})
    db.commit()

    return {"message": "Execução iniciada com sucesso"}


@router.post("/concluir/{solicitacao_id}")
def concluir_execucao(solicitacao_id: int, db: Session = Depends(get_db)):
    query_execucao = text("""
        UPDATE saf_execucoes
        SET
            status_execucao = 'CONCLUIDO',
            data_fim = CURRENT_TIMESTAMP,
            observacao_execucao = COALESCE(observacao_execucao, 'Execução concluída')
        WHERE solicitacao_id = :solicitacao_id
    """)

    query_solicitacao = text("""
        UPDATE saf_solicitacoes
        SET
            status_atual = 'FINALIZADO',
            etapa_atual = 'FINALIZADO',
            data_atualizacao = CURRENT_TIMESTAMP
        WHERE id = :solicitacao_id
    """)

    db.execute(query_execucao, {"solicitacao_id": solicitacao_id})
    db.execute(query_solicitacao, {"solicitacao_id": solicitacao_id})
    db.commit()

    return {"message": "Execução concluída com sucesso"}