from sqlalchemy.orm import Session
from sqlalchemy import text


def criar_saf(db: Session, payload: dict):
    query = text("""
        SELECT fn_abrir_saf(
            CAST(:codigo AS VARCHAR),
            CAST(:tipo AS VARCHAR),
            CURRENT_DATE,
            CAST(:supervisor AS VARCHAR),
            CAST(:atendente AS VARCHAR),
            CAST(:usuario_id AS BIGINT)
        )
    """)

    result = db.execute(query, {
        "codigo": payload["codigo_saf"],
        "tipo": payload["tipo"],
        "supervisor": payload["supervisor"],
        "atendente": payload["atendente"],
        "usuario_id": payload["usuario_id"]
    })

    novo_id = result.scalar()

    dados = payload.get("dados") or {}
    tipo = payload["tipo"]

    if tipo == "INATIVAR_CLIENTE":
        db.execute(text("""
            INSERT INTO saf_inativar_cliente (
                solicitacao_id,
                codigo_cliente,
                observacao
            )
            VALUES (
                :solicitacao_id,
                :codigo_cliente,
                :observacao
            )
        """), {
            "solicitacao_id": novo_id,
            "codigo_cliente": dados.get("cliente_codigo"),
            "observacao": dados.get("motivo")
        })

    elif tipo == "ALTERACAO_DC":
        db.execute(text("""
            INSERT INTO saf_alteracao_dc (
                solicitacao_id,
                dc_atual,
                novo_dc
            )
            VALUES (
                :solicitacao_id,
                :dc_atual,
                :novo_dc
            )
        """), {
            "solicitacao_id": novo_id,
            "dc_atual": dados.get("dc_atual"),
            "novo_dc": dados.get("dc_novo")
        })

    elif tipo == "RETORNO_MERCADORIA":
        db.execute(text("""
            INSERT INTO saf_retorno_mercadoria (
                solicitacao_id,
                nota,
                motivo
            )
            VALUES (
                :solicitacao_id,
                :nota,
                :motivo
            )
        """), {
            "solicitacao_id": novo_id,
            "nota": dados.get("nota_fiscal"),
            "motivo": dados.get("motivo_retorno")
        })

    db.commit()

    return {
        "message": "SAF criada com sucesso",
        "saf_id": novo_id
    }