from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from starlette.templating import Jinja2Templates

from app.db.session import SessionLocal
from app.services.auth_service import login as login_service

router = APIRouter(tags=["Web"])
templates = Jinja2Templates(directory="app/templates")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_usuario_logado(request: Request):
    return request.session.get("usuario")


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(
        request,
        "login.html",
        {"erro": None}
    )


@router.post("/login", response_class=HTMLResponse)
def login_web(
    request: Request,
    codigo_usuario: str = Form(...),
    senha: str = Form(...),
    db: Session = Depends(get_db)
):
    user = login_service(db, codigo_usuario, senha)

    if not user:
        return templates.TemplateResponse(
            request,
            "login.html",
            {"erro": "Usuário ou senha inválidos"}
        )

    request.session["usuario"] = {
        "id": user.id,
        "codigo_usuario": user.codigo_usuario,
        "nome": user.nome,
        "tipo": "USUARIO"
    }

    return RedirectResponse(url="/dashboard", status_code=303)


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    user = get_usuario_logado(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

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
        JOIN (
            SELECT solicitacao_id, MIN(ordem_nivel) AS proximo_nivel
            FROM saf_aprovacoes
            WHERE status_aprovacao = 'PENDENTE'
            GROUP BY solicitacao_id
        ) prox
            ON a.solicitacao_id = prox.solicitacao_id
           AND a.ordem_nivel = prox.proximo_nivel
        WHERE a.status_aprovacao = 'PENDENTE'
        ORDER BY s.id
    """)

    safs = db.execute(query).mappings().all()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "safs": safs,
            "user": user
        }
    )


@router.get("/execucao", response_class=HTMLResponse)
def tela_execucao(request: Request, db: Session = Depends(get_db)):
    user = get_usuario_logado(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    query = text("""
        SELECT
            e.id,
            e.solicitacao_id,
            s.codigo_saf,
            s.tipo_solicitacao,
            e.status_execucao,
            e.data_inicio,
            e.data_fim,
            e.observacao_execucao
        FROM saf_execucoes e
        JOIN saf_solicitacoes s
            ON s.id = e.solicitacao_id
        ORDER BY e.id DESC
    """)

    execucoes = db.execute(query).mappings().all()

    return templates.TemplateResponse(
        request,
        "execucao.html",
        {
            "execucoes": execucoes,
            "user": user
        }
    )


@router.get("/nova-saf", response_class=HTMLResponse)
def tela_nova_saf(request: Request, db: Session = Depends(get_db)):
    user = get_usuario_logado(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    query = text("""
        SELECT COALESCE(MAX(id), 0) + 1 AS proximo_id
        FROM saf_solicitacoes
    """)
    proximo_id = db.execute(query).scalar()
    codigo_sugerido = f"SAF{str(proximo_id).zfill(3)}"

    return templates.TemplateResponse(
        request,
        "nova_saf.html",
        {
            "codigo_sugerido": codigo_sugerido,
            "user": user
        }
    )


@router.get("/saf/{saf_id}", response_class=HTMLResponse)
def detalhe_saf(saf_id: int, request: Request, db: Session = Depends(get_db)):
    user = get_usuario_logado(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    saf = db.execute(text("""
        SELECT *
        FROM saf_solicitacoes
        WHERE id = :id
    """), {"id": saf_id}).mappings().first()

    aprovacoes = db.execute(text("""
        SELECT *
        FROM saf_aprovacoes
        WHERE solicitacao_id = :id
        ORDER BY ordem_nivel
    """), {"id": saf_id}).mappings().all()

    execucao = db.execute(text("""
        SELECT *
        FROM saf_execucoes
        WHERE solicitacao_id = :id
    """), {"id": saf_id}).mappings().first()

    auditoria = db.execute(text("""
        SELECT *
        FROM saf_auditoria
        WHERE solicitacao_id = :id
        ORDER BY data_hora
    """), {"id": saf_id}).mappings().all()

    dados_especificos = None

    if saf:
        tipo = saf["tipo_solicitacao"]

        if tipo == "INATIVAR_CLIENTE":
            dados_especificos = db.execute(text("""
                SELECT * FROM saf_inativar_cliente
                WHERE solicitacao_id = :id
            """), {"id": saf_id}).mappings().first()

        elif tipo == "ALTERACAO_DC":
            dados_especificos = db.execute(text("""
                SELECT * FROM saf_alteracao_dc
                WHERE solicitacao_id = :id
            """), {"id": saf_id}).mappings().first()

        elif tipo == "RETORNO_MERCADORIA":
            dados_especificos = db.execute(text("""
                SELECT * FROM saf_retorno_mercadoria
                WHERE solicitacao_id = :id
            """), {"id": saf_id}).mappings().first()

    timeline = []

    if saf:
        timeline.append({
            "etapa": "CRIADA",
            "status": "OK"
        })

    for a in aprovacoes:
        status = "OK" if a["status_aprovacao"] == "APROVADO" else a["status_aprovacao"]
        timeline.append({
            "etapa": f"APROVAÇÃO NÍVEL {a['ordem_nivel']}",
            "status": status
        })

    if execucao:
        timeline.append({
            "etapa": "EXECUÇÃO",
            "status": execucao["status_execucao"]
        })

    if saf and saf.get("status_atual") == "FINALIZADO":
        timeline.append({
            "etapa": "FINALIZADA",
            "status": "OK"
        })

    return templates.TemplateResponse(
        request,
        "detalhe_saf.html",
        {
            "saf": saf,
            "aprovacoes": aprovacoes,
            "execucao": execucao,
            "dados": dados_especificos,
            "timeline": timeline,
            "auditoria": auditoria,
            "user": user
        }
    )