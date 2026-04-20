from sqlalchemy.orm import Session
from sqlalchemy import text

def aprovar(db: Session, data: dict):
    query = text("""
        UPDATE saf_aprovacoes
        SET
            status_aprovacao = :status,
            usuario_id = :usuario_id,
            nome_assinante = :nome,
            codigo_assinatura = :codigo,
            data_assinatura = CURRENT_TIMESTAMP
        WHERE solicitacao_id = :saf_id
          AND ordem_nivel = :nivel
    """)

    db.execute(query, {
        "status": data["status"],
        "usuario_id": data["usuario_id"],
        "nome": data["nome"],
        "codigo": data["codigo"],
        "saf_id": data["saf_id"],
        "nivel": data["nivel"]
    })

    db.commit()
    return {"message": "Aprovação realizada"}