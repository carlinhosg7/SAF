# -*- coding: utf-8 -*-
import os
import math
import json
import unicodedata
from functools import wraps
from datetime import datetime
import time
from typing import Optional
from uuid import uuid4
from pathlib import Path
from werkzeug.utils import secure_filename
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
import smtplib

import bcrypt
import pandas as pd
import psycopg2
import requests
from html import escape
from psycopg2.extras import execute_batch, RealDictCursor
from flask import Flask, request, redirect, session, render_template_string, abort, jsonify, send_file, url_for, flash, get_flashed_messages
from saf_tipos.inativar_cliente import render_nova_saf_inativar_cliente
from saf_tipos.comum import buscar_representante_cache, buscar_clientes_por_representante
from saf_tipos.alterar_portador_devolucao import render_nova_saf_alterar_portador_devolucao
from saf_tipos.prorrogar_sem_juros import render_nova_saf_prorrogar_sem_juros
from saf_tipos.prorrogar_com_juros import render_nova_saf_prorrogar_com_juros


def carregar_arquivo_env_local():
    """
    Carrega variáveis de ambiente de arquivos locais .env/.env.local sem depender
    de bibliotecas externas. As variáveis já existentes no ambiente do sistema têm
    prioridade e não são sobrescritas.
    """
    candidatos = [
        Path(".env"),
        Path(".env.local"),
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent / ".env.local",
    ]
    for arquivo in candidatos:
        try:
            if not arquivo.exists() or not arquivo.is_file():
                continue
            for linha in arquivo.read_text(encoding="utf-8").splitlines():
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                chave, valor = linha.split("=", 1)
                chave = chave.strip()
                valor = valor.strip().strip('"').strip("'")
                if chave and chave not in os.environ:
                    os.environ[chave] = valor
        except Exception as e:
            print(f"[SAF][ENV] Falha ao ler {arquivo}: {e}")

carregar_arquivo_env_local()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "super-secret-key")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "database": os.getenv("DB_NAME", "saf_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "123456"),
    "port": int(os.getenv("DB_PORT", "5432")),
}

ARQ_CLIENTES = r"D:\SAF\Arquivos\CLIENTES.xlsx"
ARQ_PEDIDOS = r"D:\SAF\Arquivos\PEDIDOS.xlsx"
ARQ_TITULOS = r"D:\SAF\Arquivos\TITULOS.xlsx"
ARQ_TITULOS2 = r"D:\SAF\Arquivos\TITULOS2.xlsx"
ARQ_LOGO_KIDY = r"D:\SAF\Arquivos\logo_kidy.png"
ARQ_LOGO_KIDY_ICON = r"D:\SAF\Arquivos\logo_kidy_icon.ico"
PASTA_ANEXOS_SAF = Path(os.getenv("SAF_DOCS_DIR", r"D:\SAF\doc"))
MAX_ANEXO_MB = int(os.getenv("SAF_MAX_ANEXO_MB", "20"))
EXTENSOES_PERMITIDAS_ANEXO = {
    ".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".svg",
    ".pdf",
    ".xls", ".xlsx", ".xlsm", ".csv", ".ods",
    ".doc", ".docx", ".odt", ".txt", ".rtf",
    ".zip", ".rar", ".7z"
}

SUPERVISORES_FIXOS = [
    "CEO",
    "REM",
    "NORTE / NORDESTE",
    "SUL",
    "SPI",
    "SPC",
]

BASES_JA_VERIFICADAS = False


WHATSAPP_PROVIDER = (os.getenv("WHATSAPP_PROVIDER", "twilio") or "twilio").strip().lower()
TWILIO_ACCOUNT_SID = (os.getenv("TWILIO_ACCOUNT_SID") or os.getenv("TWILIO_SID") or "").strip()
TWILIO_AUTH_TOKEN = (os.getenv("TWILIO_AUTH_TOKEN") or os.getenv("TWILIO_TOKEN") or "").strip()
TWILIO_WHATSAPP_FROM = (os.getenv("TWILIO_WHATSAPP_FROM") or os.getenv("TWILIO_FROM") or "whatsapp:+14155238886").strip()
TWILIO_CONTENT_SID = (os.getenv("TWILIO_CONTENT_SID") or os.getenv("TWILIO_TEMPLATE_SID") or "").strip()
WHATSAPP_DEFAULT_COUNTRY_CODE = (os.getenv("WHATSAPP_DEFAULT_COUNTRY_CODE") or "+55").strip()
WHATSAPP_TIMEOUT = int((os.getenv("WHATSAPP_TIMEOUT") or "20").strip())
TWILIO_STATUS_POLL_SECONDS = float((os.getenv("TWILIO_STATUS_POLL_SECONDS") or "3").strip())
TWILIO_STATUS_POLL_ATTEMPTS = int((os.getenv("TWILIO_STATUS_POLL_ATTEMPTS") or "2").strip())

EMAIL_HOST = (os.getenv("EMAIL_HOST") or "smtp.office365.com").strip()
EMAIL_PORT = int((os.getenv("EMAIL_PORT") or "587").strip())
EMAIL_USER = (os.getenv("EMAIL_USER") or os.getenv("SMTP_USER") or "").strip()
EMAIL_PASS = (os.getenv("EMAIL_PASS") or os.getenv("SMTP_PASS") or "").strip()
EMAIL_FROM = (os.getenv("EMAIL_FROM") or os.getenv("SMTP_FROM") or EMAIL_USER).strip()
EMAIL_FROM_NAME = (os.getenv("EMAIL_FROM_NAME") or os.getenv("SMTP_FROM_NAME") or "").strip()
EMAIL_REPLY_TO = (os.getenv("EMAIL_REPLY_TO") or os.getenv("SMTP_REPLY_TO") or "").strip()
EMAIL_USE_TLS = str(os.getenv("EMAIL_USE_TLS", "true") or "true").strip().lower() in {"1", "true", "sim", "yes", "y"}
EMAIL_USE_SSL = str(os.getenv("EMAIL_USE_SSL", "false") or "false").strip().lower() in {"1", "true", "sim", "yes", "y"}
EMAIL_TIMEOUT = int((os.getenv("EMAIL_TIMEOUT") or "20").strip())

WHATSAPP_DESTINOS_FIXOS = {
    'supervisor': {'codigo_usuario': 'FIXO_SUPERVISOR', 'nome_usuario': 'Supervisor', 'perfil': 'supervisor', 'telefone_whatsapp': '5518996253519', 'email': None},
    'gerente': {'codigo_usuario': 'FIXO_GERENTE', 'nome_usuario': 'Gerente', 'perfil': 'gerente', 'telefone_whatsapp': '5518996253519', 'email': None},
    'diretor': {'codigo_usuario': 'FIXO_DIRETOR', 'nome_usuario': 'Diretor', 'perfil': 'diretor', 'telefone_whatsapp': '5518996253519', 'email': None},
    'supervisor_financeiro': {'codigo_usuario': 'FIXO_SUPERVISOR_FINANCEIRO', 'nome_usuario': 'Supervisor Financeiro', 'perfil': 'supervisor_financeiro', 'telefone_whatsapp': '5518996253519', 'email': None},
    'financeiro': {'codigo_usuario': 'FIXO_FINANCEIRO', 'nome_usuario': 'Financeiro', 'perfil': 'financeiro', 'telefone_whatsapp': '5518996253519', 'email': None},
    'atendente': {'codigo_usuario': 'FIXO_ATENDENTE', 'nome_usuario': 'Atendente', 'perfil': 'atendente', 'telefone_whatsapp': '5518998176101', 'email': None},
}


# =========================
# CONEXAO
# =========================
def get_conn():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        port=DB_CONFIG["port"],
    )


def gerar_hash_senha(senha: str) -> str:
    return bcrypt.hashpw(str(senha).encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verificar_senha(senha: str, senha_hash: str) -> bool:
    try:
        return bcrypt.checkpw(str(senha).encode("utf-8"), str(senha_hash).encode("utf-8"))
    except Exception:
        return False


def obter_configuracoes_sistema() -> dict:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT chave, valor FROM saf_configuracoes_sistema")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {str(chave or '').strip(): valor for chave, valor in rows}
    except Exception:
        return {}


def salvar_configuracoes_sistema(configs: dict):
    if not configs:
        return
    conn = get_conn()
    cur = conn.cursor()
    for chave, valor in configs.items():
        cur.execute(
            """
            INSERT INTO saf_configuracoes_sistema (chave, valor, atualizado_por_codigo, atualizado_por_nome, atualizado_em)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (chave)
            DO UPDATE SET
                valor = EXCLUDED.valor,
                atualizado_por_codigo = EXCLUDED.atualizado_por_codigo,
                atualizado_por_nome = EXCLUDED.atualizado_por_nome,
                atualizado_em = CURRENT_TIMESTAMP
            """,
            (str(chave), valor, usuario_logado_codigo() or None, usuario_logado_nome() or None)
        )
    conn.commit()
    cur.close()
    conn.close()


def config_sistema(chave: str, default=None):
    configs = obter_configuracoes_sistema()
    valor = configs.get(chave)
    if valor is None or str(valor).strip() == '':
        return default
    return valor


def obter_config_runtime_notificacoes() -> dict:
    provider = (config_sistema('WHATSAPP_PROVIDER', WHATSAPP_PROVIDER) or WHATSAPP_PROVIDER or 'twilio').strip().lower()
    account_sid = (config_sistema('TWILIO_ACCOUNT_SID', TWILIO_ACCOUNT_SID) or TWILIO_ACCOUNT_SID or '').strip()
    auth_token = (config_sistema('TWILIO_AUTH_TOKEN', TWILIO_AUTH_TOKEN) or TWILIO_AUTH_TOKEN or '').strip()
    whatsapp_from = (config_sistema('TWILIO_WHATSAPP_FROM', TWILIO_WHATSAPP_FROM) or TWILIO_WHATSAPP_FROM or '').strip()
    content_sid = (config_sistema('TWILIO_CONTENT_SID', TWILIO_CONTENT_SID) or TWILIO_CONTENT_SID or '').strip()
    default_country_code = (config_sistema('WHATSAPP_DEFAULT_COUNTRY_CODE', WHATSAPP_DEFAULT_COUNTRY_CODE) or WHATSAPP_DEFAULT_COUNTRY_CODE or '+55').strip()
    timeout_raw = config_sistema('WHATSAPP_TIMEOUT', str(WHATSAPP_TIMEOUT))
    poll_seconds_raw = config_sistema('TWILIO_STATUS_POLL_SECONDS', str(TWILIO_STATUS_POLL_SECONDS))
    poll_attempts_raw = config_sistema('TWILIO_STATUS_POLL_ATTEMPTS', str(TWILIO_STATUS_POLL_ATTEMPTS))
    email_host = (config_sistema('EMAIL_HOST', EMAIL_HOST) or EMAIL_HOST or '').strip()
    email_port_raw = config_sistema('EMAIL_PORT', str(EMAIL_PORT))
    email_user = (config_sistema('EMAIL_USER', EMAIL_USER) or EMAIL_USER or '').strip()
    email_pass = (config_sistema('EMAIL_PASS', EMAIL_PASS) or EMAIL_PASS or '').strip()
    email_from = (config_sistema('EMAIL_FROM', EMAIL_FROM) or EMAIL_FROM or email_user or '').strip()
    email_from_name = (config_sistema('EMAIL_FROM_NAME', EMAIL_FROM_NAME) or EMAIL_FROM_NAME or '').strip()
    email_reply_to = (config_sistema('EMAIL_REPLY_TO', EMAIL_REPLY_TO) or EMAIL_REPLY_TO or '').strip()
    email_use_tls_raw = str(config_sistema('EMAIL_USE_TLS', str(EMAIL_USE_TLS).lower()) or str(EMAIL_USE_TLS).lower()).strip().lower()
    email_use_ssl_raw = str(config_sistema('EMAIL_USE_SSL', str(EMAIL_USE_SSL).lower()) or str(EMAIL_USE_SSL).lower()).strip().lower()
    email_timeout_raw = config_sistema('EMAIL_TIMEOUT', str(EMAIL_TIMEOUT))
    try:
        whatsapp_timeout = int(str(timeout_raw).strip())
    except Exception:
        whatsapp_timeout = WHATSAPP_TIMEOUT
    try:
        poll_seconds = float(str(poll_seconds_raw).strip())
    except Exception:
        poll_seconds = TWILIO_STATUS_POLL_SECONDS
    try:
        poll_attempts = int(str(poll_attempts_raw).strip())
    except Exception:
        poll_attempts = TWILIO_STATUS_POLL_ATTEMPTS
    try:
        email_port = int(str(email_port_raw).strip())
    except Exception:
        email_port = EMAIL_PORT
    try:
        email_timeout = int(str(email_timeout_raw).strip())
    except Exception:
        email_timeout = EMAIL_TIMEOUT
    email_use_tls = email_use_tls_raw in {'1', 'true', 'sim', 'yes', 'y'}
    email_use_ssl = email_use_ssl_raw in {'1', 'true', 'sim', 'yes', 'y'}
    return {
        'WHATSAPP_PROVIDER': provider,
        'TWILIO_ACCOUNT_SID': account_sid,
        'TWILIO_AUTH_TOKEN': auth_token,
        'TWILIO_WHATSAPP_FROM': whatsapp_from,
        'TWILIO_CONTENT_SID': content_sid,
        'WHATSAPP_DEFAULT_COUNTRY_CODE': default_country_code,
        'WHATSAPP_TIMEOUT': whatsapp_timeout,
        'TWILIO_STATUS_POLL_SECONDS': poll_seconds,
        'TWILIO_STATUS_POLL_ATTEMPTS': poll_attempts,
        'EMAIL_HOST': email_host,
        'EMAIL_PORT': email_port,
        'EMAIL_USER': email_user,
        'EMAIL_PASS': email_pass,
        'EMAIL_FROM': email_from,
        'EMAIL_FROM_NAME': email_from_name,
        'EMAIL_REPLY_TO': email_reply_to,
        'EMAIL_USE_TLS': email_use_tls,
        'EMAIL_USE_SSL': email_use_ssl,
        'EMAIL_TIMEOUT': email_timeout,
    }


# =========================
# AUTH / PERFIL
# =========================
def get_role(codigo: str) -> str:
    codigo = (codigo or "").strip().upper()
    if codigo.startswith("ATD"):
        return "atendente"
    if codigo.startswith("SUP") or codigo.startswith("COR"):
        return "supervisor"
    if codigo.startswith("SFI"):
        return "supervisor_financeiro"
    if codigo.startswith("GER"):
        return "gerente"
    if codigo.startswith("DIR"):
        return "diretor"
    if codigo.startswith("FIN"):
        return "financeiro"
    if codigo.startswith("ADM"):
        return "admin"
    return "usuario"


def normalizar_nivel_para_role(nivel: str) -> str:
    nivel = (nivel or "").strip().lower()
    mapa = {
        "admin": "admin",
        "atendente": "atendente",
        "supervisor": "supervisor",
        "gerente": "gerente",
        "diretor": "diretor",
        "financeiro": "financeiro",
        "supervisor_financeiro": "supervisor_financeiro",
    }
    return mapa.get(nivel, "usuario")


def lista_regionais_usuario() -> list[str]:
    regional = str(session.get("regional", "") or "").strip()
    if not regional:
        return []
    return [parte.strip().upper() for parte in regional.split(",") if parte.strip()]


def saf_pertence_ao_supervisor(saf: dict) -> bool:
    supervisor_saf = str(saf.get("supervisor") or "").strip().upper()
    regionais = lista_regionais_usuario()
    return bool(supervisor_saf) and supervisor_saf in regionais


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated


def role_required(*roles):
    def wrapper(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if "role" not in session or session["role"] not in roles:
                abort(403)
            return f(*args, **kwargs)
        return decorated
    return wrapper


# =========================
# FORMATACAO
# =========================
def formatar_data(valor):
    if not valor:
        return "-"
    return valor.strftime("%d/%m/%Y %H:%M")


def formatar_moeda(valor):
    numero = float(valor or 0)
    return f"{numero:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def garantir_pasta_anexos():
    PASTA_ANEXOS_SAF.mkdir(parents=True, exist_ok=True)


def extensao_permitida_anexo(nome_arquivo: str) -> bool:
    ext = Path(nome_arquivo or "").suffix.lower()
    return ext in EXTENSOES_PERMITIDAS_ANEXO


def tamanho_legivel(bytes_total: Optional[int]) -> str:
    try:
        size = float(bytes_total or 0)
    except Exception:
        return "0 B"
    unidades = ["B", "KB", "MB", "GB"]
    idx = 0
    while size >= 1024 and idx < len(unidades) - 1:
        size /= 1024
        idx += 1
    return f"{size:.1f} {unidades[idx]}" if idx else f"{int(size)} {unidades[idx]}"


def icone_anexo(nome_arquivo: Optional[str]) -> str:
    ext = Path(nome_arquivo or "").suffix.lower()
    if ext in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".svg"}:
        return "🖼️"
    if ext == ".pdf":
        return "📕"
    if ext in {".xls", ".xlsx", ".xlsm", ".csv", ".ods"}:
        return "📊"
    if ext in {".doc", ".docx", ".odt", ".txt", ".rtf"}:
        return "📄"
    if ext in {".zip", ".rar", ".7z"}:
        return "🗜️"
    return "📎"


# =========================
# EXCEL / NORMALIZACAO
# =========================
def normalizar_nome_coluna(col: str) -> str:
    col = str(col).strip().lower()
    col = unicodedata.normalize("NFKD", col).encode("ascii", "ignore").decode("ascii")
    col = col.replace("/", "_").replace("-", "_").replace(".", "_")
    while "  " in col:
        col = col.replace("  ", " ")
    col = col.replace(" ", "_")
    while "__" in col:
        col = col.replace("__", "_")
    return col.strip("_")


def achar_coluna(df: pd.DataFrame, candidatos: list[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidatos:
        if c in cols:
            return c
    return None


def ler_excel_inteligente(path: str) -> pd.DataFrame:
    bruto = pd.read_excel(path, header=None, dtype=object)

    melhor_df = None
    melhor_score = -1

    for header_row in [0, 1, 2]:
        if header_row >= len(bruto):
            continue

        df = bruto.copy()
        df.columns = [normalizar_nome_coluna(c) for c in df.iloc[header_row].tolist()]
        df = df.iloc[header_row + 1:].reset_index(drop=True)

        cols = set(df.columns)
        score = 0

        palavras_importantes = {
            "codigo_cliente",
            "razao_social",
            "numero_pedido",
            "documento",
            "data_vencimento",
            "supervisor",
            "representante_carteira",
            "codigo_representante_carteira",
        }

        for p in palavras_importantes:
            if p in cols:
                score += 1

        if score > melhor_score:
            melhor_score = score
            melhor_df = df

    if melhor_df is None:
        raise ValueError(f"Não foi possível identificar cabeçalho válido em: {path}")

    melhor_df.columns = [normalizar_nome_coluna(c) for c in melhor_df.columns]
    return melhor_df


def valor_texto(v):
    if pd.isna(v):
        return None
    v = str(v).strip()
    return v if v else None


def valor_numerico(v):
    if pd.isna(v):
        return None

    if isinstance(v, str):
        v = v.strip().replace(".", "").replace(",", ".")
        if v == "":
            return None

    try:
        n = float(v)
        if math.isnan(n):
            return None
        return n
    except Exception:
        return None


def valor_data(v):
    if pd.isna(v):
        return None
    try:
        dt = pd.to_datetime(v, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None




# =========================
# ESTRUTURA SAF
# =========================
TIPOS_SAF_VALIDOS = [
    "ALTERAR_PORTADOR_DEVOLUCAO",
    "PRORROGAR_SEM_JUROS",
    "INATIVAR_CLIENTE",
    "ALTERAR_PORTADOR_PARA_DEVOLUCAO",
    "ALTERAR_PORTADOR_DIVERSOS",
    "PRORROGAR_COM_JUROS",
    "NEGOCIACAO_TITULOS_REPARCELAMENTO",
    "BAIXAR_CREDITO_CLIENTE",
    "CREDITAR_CLIENTE",
    "CARTA_ANUENCIA_CLIENTE",
    "DESCONTOS_DIVERSOS",
]


def garantir_constraint_tipo_saf(cur):
    mapa_legado = {
        "ALTERAR_PORTADOR_PARA_DEVOLUCAO": "ALTERAR_PORTADOR_DEVOLUCAO",
        "ALTERAR PORTADOR PARA DEVOLUCAO": "ALTERAR_PORTADOR_DEVOLUCAO",
        "ALTERAR PORTADOR DEVOLUCAO": "ALTERAR_PORTADOR_DEVOLUCAO",
        "PRORROGAR SEM JUROS": "PRORROGAR_SEM_JUROS",
        "PRORROGAR COM JUROS": "PRORROGAR_COM_JUROS",
        "NEGOCIACAO DE TITULOS - REPARCELAMENTO": "NEGOCIACAO_TITULOS_REPARCELAMENTO",
        "NEGOCIACAO_TITULOS_REPARCELAMENTO": "NEGOCIACAO_TITULOS_REPARCELAMENTO",
        "BAIXAR CREDITO DO CLIENTE": "BAIXAR_CREDITO_CLIENTE",
        "BAIXAR_CREDITO_DO_CLIENTE": "BAIXAR_CREDITO_CLIENTE",
        "BAIXAR_CREDITO_CLIENTE": "BAIXAR_CREDITO_CLIENTE",
        "CREDITAR O CLIENTE": "CREDITAR_CLIENTE",
        "CREDITAR_CLIENTE": "CREDITAR_CLIENTE",
        "CARTA DE ANUENCIA": "CARTA_ANUENCIA_CLIENTE",
        "CARTA_ANUENCIA": "CARTA_ANUENCIA_CLIENTE",
        "CARTA_ANUENCIA_CLIENTE": "CARTA_ANUENCIA_CLIENTE",
        "DESCONTOS DIVERSOS": "DESCONTOS_DIVERSOS",
        "DESCONTOS_DIVERSOS": "DESCONTOS_DIVERSOS",
        "INATIVAR CLIENTE": "INATIVAR_CLIENTE",
        "INATIVAR_CLIENTE": "INATIVAR_CLIENTE",
    }

    for origem, destino in mapa_legado.items():
        cur.execute(
            """
            UPDATE saf_solicitacoes
               SET tipo_saf = %s
             WHERE UPPER(BTRIM(COALESCE(tipo_saf, ''))) = %s
            """,
            (destino, origem),
        )

    cur.execute("""
        ALTER TABLE saf_solicitacoes
        DROP CONSTRAINT IF EXISTS chk_saf_solicitacoes_tipo_saf
    """)

    cur.execute(
        """
        SELECT DISTINCT UPPER(BTRIM(COALESCE(tipo_saf, ''))) AS tipo
          FROM saf_solicitacoes
         WHERE BTRIM(COALESCE(tipo_saf, '')) <> ''
         ORDER BY 1
        """
    )
    tipos_existentes = [str(row[0] or '').strip().upper() for row in cur.fetchall() if str(row[0] or '').strip()]

    tipos_permitidos = []
    for tipo in [t.upper() for t in TIPOS_SAF_VALIDOS] + tipos_existentes:
        if tipo and tipo not in tipos_permitidos:
            tipos_permitidos.append(tipo)

    tipos_sql = ", ".join(["%s"] * len(tipos_permitidos))
    cur.execute(
        f"""
        ALTER TABLE saf_solicitacoes
        ADD CONSTRAINT chk_saf_solicitacoes_tipo_saf
        CHECK (UPPER(COALESCE(tipo_saf, '')) IN ({tipos_sql}))
        """,
        tipos_permitidos,
    )


def garantir_tabelas_saf():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS saf_solicitacoes (
            id SERIAL PRIMARY KEY,
            tipo_saf VARCHAR(60) NOT NULL,
            data_solicitacao DATE NOT NULL DEFAULT CURRENT_DATE,
            supervisor VARCHAR(100),
            codigo_representante VARCHAR(50),
            representante VARCHAR(255),
            atendente_codigo VARCHAR(50),
            atendente_nome VARCHAR(255),
            codigo_cliente VARCHAR(50),
            razao_social VARCHAR(255),
            ocorrencia_geral TEXT,
            prioridade VARCHAR(20) NOT NULL DEFAULT 'NORMAL',
            status VARCHAR(50) NOT NULL DEFAULT 'PENDENTE_SUPERVISOR',
            criado_por_codigo VARCHAR(50),
            criado_por_nome VARCHAR(255),
            perfil_criador VARCHAR(50),
            ultima_acao_aprovacao VARCHAR(50),
            ultima_observacao_aprovacao TEXT,
            ultimo_aprovador_codigo VARCHAR(50),
            ultimo_aprovador_nome VARCHAR(255),
            perfil_aprovador VARCHAR(50),
            data_aprovacao TIMESTAMP,
            criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for ddl in [
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS data_solicitacao DATE NOT NULL DEFAULT CURRENT_DATE",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS supervisor VARCHAR(100)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS codigo_representante VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS representante VARCHAR(255)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS atendente_codigo VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS atendente_nome VARCHAR(255)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS codigo_cliente VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS razao_social VARCHAR(255)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS ocorrencia_geral TEXT",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS prioridade VARCHAR(20) NOT NULL DEFAULT 'NORMAL'",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS status VARCHAR(50) NOT NULL DEFAULT 'PENDENTE_SUPERVISOR'",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS criado_por_codigo VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS criado_por_nome VARCHAR(255)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS perfil_criador VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS ultima_acao_aprovacao VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS ultima_observacao_aprovacao TEXT",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS ultimo_aprovador_codigo VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS ultimo_aprovador_nome VARCHAR(255)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS perfil_aprovador VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS data_aprovacao TIMESTAMP",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS executado_por_codigo VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS executado_por_nome VARCHAR(255)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS executado_por_perfil VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS data_execucao TIMESTAMP",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS observacao_execucao TEXT",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS autenticado_por_codigo VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS autenticado_por_nome VARCHAR(255)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS autenticado_por_perfil VARCHAR(50)",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS data_autenticacao TIMESTAMP",
        "ALTER TABLE saf_solicitacoes ADD COLUMN IF NOT EXISTS observacao_autenticacao TEXT",
    ]:
        cur.execute(ddl)

    cur.execute("""
        UPDATE saf_solicitacoes
           SET prioridade = COALESCE(NULLIF(BTRIM(prioridade), ''), 'NORMAL'),
               criado_por_codigo = COALESCE(criado_por_codigo, atendente_codigo),
               criado_por_nome = COALESCE(criado_por_nome, atendente_nome),
               perfil_criador = COALESCE(perfil_criador, 'atendente'),
               status = CASE
                    WHEN status IS NULL OR BTRIM(status) = '' THEN 'PENDENTE_SUPERVISOR'
                    WHEN status = 'PENDENTE' THEN 'PENDENTE_SUPERVISOR'
                    WHEN status = 'APROVADO_FINAL' THEN 'FINALIZADO'
                    WHEN status = 'APROVADO' AND UPPER(COALESCE(perfil_aprovador, '')) = 'DIRETOR' AND UPPER(COALESCE(tipo_saf, '')) IN ('ALTERACAO_TITULOS', 'ALTERACAO_DE_TITULOS', 'TITULOS', 'TITULO', 'ALTERAR_TITULOS', 'TITULOS_DEVOLUCAO') THEN 'PENDENTE_SUPERVISOR_FINANCEIRO'
                    WHEN status = 'APROVADO' AND UPPER(COALESCE(perfil_aprovador, '')) = 'DIRETOR' THEN 'PENDENTE_EXECUCAO_ATENDENTE'
                    WHEN status = 'APROVADO' AND UPPER(COALESCE(perfil_aprovador, '')) = 'GERENTE' THEN 'PENDENTE_DIRETOR'
                    WHEN status = 'APROVADO' AND UPPER(COALESCE(perfil_aprovador, '')) IN ('COORDENADOR','SUPERVISOR') THEN 'PENDENTE_GERENTE'
                    ELSE status
               END
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS saf_itens (
            id SERIAL PRIMARY KEY,
            saf_id INTEGER NOT NULL REFERENCES saf_solicitacoes(id) ON DELETE CASCADE,
            ordem INTEGER NOT NULL,
            tipo_saf VARCHAR(60) NOT NULL,
            pedido VARCHAR(100),
            dc DATE,
            pares NUMERIC(18,2),
            valor NUMERIC(18,2),
            novo_dc VARCHAR(100),
            titulo VARCHAR(100),
            vencimento DATE,
            novo_portador VARCHAR(255),
            despesas_financeiras NUMERIC(18,2),
            total NUMERIC(18,2),
            situacao VARCHAR(255),
            acao VARCHAR(255),
            ocorrencia_item TEXT
        )
    """)

    for ddl in [
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS saf_id INTEGER",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS ordem INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS tipo_saf VARCHAR(60)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS pedido VARCHAR(100)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS dc DATE",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS pares NUMERIC(18,2)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS valor NUMERIC(18,2)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS novo_dc VARCHAR(100)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS titulo VARCHAR(100)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS vencimento DATE",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS novo_portador VARCHAR(255)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS despesas_financeiras NUMERIC(18,2)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS total NUMERIC(18,2)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS situacao VARCHAR(255)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS acao VARCHAR(255)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS ocorrencia_item TEXT",
        # ✅ COLUNAS FALTANTES — causavam erro no INSERT
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS codigo_cliente_item VARCHAR(50)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS razao_social_item VARCHAR(255)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS cnpj_item VARCHAR(30)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS codigo_grupo_cliente_item VARCHAR(50)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS grupo_cliente_item VARCHAR(255)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS vencimento_original DATE",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS novo_vencimento DATE",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS data_faturamento DATE",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS data_saida DATE",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS data_expedicao DATE",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS data_prevista_entrega DATE",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS percentual_juros NUMERIC(18,4)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS valor_juros NUMERIC(18,2)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS portador_atual VARCHAR(255)",
        "ALTER TABLE saf_itens ADD COLUMN IF NOT EXISTS carteira_descricao VARCHAR(255)",
]:
        cur.execute(ddl)
    
     
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saf_aprovacoes (
            id SERIAL PRIMARY KEY,
            saf_id INTEGER NOT NULL REFERENCES saf_solicitacoes(id) ON DELETE CASCADE,
            acao VARCHAR(30) NOT NULL,
            observacao TEXT,
            usuario_codigo VARCHAR(50),
            usuario_nome VARCHAR(255),
            usuario_perfil VARCHAR(50),
            criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for ddl in [
        "ALTER TABLE saf_aprovacoes ADD COLUMN IF NOT EXISTS saf_id INTEGER",
        "ALTER TABLE saf_aprovacoes ADD COLUMN IF NOT EXISTS acao VARCHAR(30)",
        "ALTER TABLE saf_aprovacoes ADD COLUMN IF NOT EXISTS observacao TEXT",
        "ALTER TABLE saf_aprovacoes ADD COLUMN IF NOT EXISTS usuario_codigo VARCHAR(50)",
        "ALTER TABLE saf_aprovacoes ADD COLUMN IF NOT EXISTS usuario_nome VARCHAR(255)",
        "ALTER TABLE saf_aprovacoes ADD COLUMN IF NOT EXISTS usuario_perfil VARCHAR(50)",
        "ALTER TABLE saf_aprovacoes ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    ]:
        cur.execute(ddl)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS saf_anexos (
            id SERIAL PRIMARY KEY,
            saf_id INTEGER NOT NULL REFERENCES saf_solicitacoes(id) ON DELETE CASCADE,
            nome_original VARCHAR(255) NOT NULL,
            nome_salvo VARCHAR(255) NOT NULL,
            caminho_arquivo TEXT NOT NULL,
            extensao VARCHAR(20),
            tamanho_bytes BIGINT,
            mime_type VARCHAR(255),
            enviado_por_codigo VARCHAR(50),
            enviado_por_nome VARCHAR(255),
            enviado_por_perfil VARCHAR(50),
            criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for ddl in [
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS saf_id INTEGER",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS nome_original VARCHAR(255)",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS nome_salvo VARCHAR(255)",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS caminho_arquivo TEXT",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS extensao VARCHAR(20)",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS tamanho_bytes BIGINT",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS mime_type VARCHAR(255)",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS enviado_por_codigo VARCHAR(50)",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS enviado_por_nome VARCHAR(255)",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS enviado_por_perfil VARCHAR(50)",
        "ALTER TABLE saf_anexos ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    ]:
        cur.execute(ddl)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS saf_anexos_log (
            id SERIAL PRIMARY KEY,
            saf_id INTEGER NOT NULL REFERENCES saf_solicitacoes(id) ON DELETE CASCADE,
            anexo_id INTEGER REFERENCES saf_anexos(id) ON DELETE SET NULL,
            acao VARCHAR(30) NOT NULL,
            nome_arquivo VARCHAR(255),
            observacao TEXT,
            usuario_codigo VARCHAR(50),
            usuario_nome VARCHAR(255),
            usuario_perfil VARCHAR(50),
            criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for ddl in [
        "ALTER TABLE saf_anexos_log ADD COLUMN IF NOT EXISTS saf_id INTEGER",
        "ALTER TABLE saf_anexos_log ADD COLUMN IF NOT EXISTS anexo_id INTEGER",
        "ALTER TABLE saf_anexos_log ADD COLUMN IF NOT EXISTS acao VARCHAR(30)",
        "ALTER TABLE saf_anexos_log ADD COLUMN IF NOT EXISTS nome_arquivo VARCHAR(255)",
        "ALTER TABLE saf_anexos_log ADD COLUMN IF NOT EXISTS observacao TEXT",
        "ALTER TABLE saf_anexos_log ADD COLUMN IF NOT EXISTS usuario_codigo VARCHAR(50)",
        "ALTER TABLE saf_anexos_log ADD COLUMN IF NOT EXISTS usuario_nome VARCHAR(255)",
        "ALTER TABLE saf_anexos_log ADD COLUMN IF NOT EXISTS usuario_perfil VARCHAR(50)",
        "ALTER TABLE saf_anexos_log ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    ]:
        cur.execute(ddl)


    cur.execute("""
        CREATE TABLE IF NOT EXISTS saf_notificacoes_usuarios (
            id SERIAL PRIMARY KEY,
            codigo_usuario VARCHAR(50) NOT NULL,
            nome_usuario VARCHAR(255),
            perfil VARCHAR(50) NOT NULL,
            telefone_whatsapp VARCHAR(30) NOT NULL,
            email VARCHAR(255),
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            recebe_criacao BOOLEAN NOT NULL DEFAULT TRUE,
            recebe_aprovacao BOOLEAN NOT NULL DEFAULT TRUE,
            recebe_reprovacao BOOLEAN NOT NULL DEFAULT TRUE,
            recebe_observacao BOOLEAN NOT NULL DEFAULT FALSE,
            criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (codigo_usuario, telefone_whatsapp)
        )
    """)

    for ddl in [
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS codigo_usuario VARCHAR(50)",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS nome_usuario VARCHAR(255)",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS perfil VARCHAR(50)",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS telefone_whatsapp VARCHAR(30)",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS email VARCHAR(255)",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS ativo BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS recebe_criacao BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS recebe_aprovacao BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS recebe_reprovacao BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS recebe_observacao BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE saf_notificacoes_usuarios ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
    ]:
        cur.execute(ddl)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS saf_notificacoes_log (
            id SERIAL PRIMARY KEY,
            saf_id INTEGER REFERENCES saf_solicitacoes(id) ON DELETE SET NULL,
            evento VARCHAR(50) NOT NULL,
            destino_codigo_usuario VARCHAR(50),
            destino_nome_usuario VARCHAR(255),
            destino_perfil VARCHAR(50),
            telefone_whatsapp VARCHAR(30),
            email_destino VARCHAR(255),
            provedor VARCHAR(30),
            status_envio VARCHAR(30) NOT NULL,
            mensagem TEXT,
            resposta_provedor TEXT,
            criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for ddl in [
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS saf_id INTEGER",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS evento VARCHAR(50)",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS destino_codigo_usuario VARCHAR(50)",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS destino_nome_usuario VARCHAR(255)",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS destino_perfil VARCHAR(50)",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS telefone_whatsapp VARCHAR(30)",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS email_destino VARCHAR(255)",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS provedor VARCHAR(30)",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS status_envio VARCHAR(30)",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS mensagem TEXT",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS resposta_provedor TEXT",
        "ALTER TABLE saf_notificacoes_log ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
    ]:
        cur.execute(ddl)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS saf_notificacoes_internas (
            id SERIAL PRIMARY KEY,
            codigo_usuario VARCHAR(50) NOT NULL,
            titulo VARCHAR(255),
            mensagem TEXT NOT NULL,
            saf_id INTEGER REFERENCES saf_solicitacoes(id) ON DELETE SET NULL,
            evento VARCHAR(50),
            lida BOOLEAN NOT NULL DEFAULT FALSE,
            criada_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            lida_em TIMESTAMP
        )
    """)

    for ddl in [
        "ALTER TABLE saf_notificacoes_internas ADD COLUMN IF NOT EXISTS codigo_usuario VARCHAR(50)",
        "ALTER TABLE saf_notificacoes_internas ADD COLUMN IF NOT EXISTS titulo VARCHAR(255)",
        "ALTER TABLE saf_notificacoes_internas ADD COLUMN IF NOT EXISTS mensagem TEXT",
        "ALTER TABLE saf_notificacoes_internas ADD COLUMN IF NOT EXISTS saf_id INTEGER",
        "ALTER TABLE saf_notificacoes_internas ADD COLUMN IF NOT EXISTS evento VARCHAR(50)",
        "ALTER TABLE saf_notificacoes_internas ADD COLUMN IF NOT EXISTS lida BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE saf_notificacoes_internas ADD COLUMN IF NOT EXISTS criada_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE saf_notificacoes_internas ADD COLUMN IF NOT EXISTS lida_em TIMESTAMP"
    ]:
        cur.execute(ddl)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_saf_notificacoes_internas_usuario ON saf_notificacoes_internas (UPPER(codigo_usuario), lida, criada_em DESC)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            codigo_usuario VARCHAR(50) NOT NULL UNIQUE,
            nome VARCHAR(255) NOT NULL,
            senha_hash TEXT,
            nivel VARCHAR(50),
            regional VARCHAR(255),
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            email VARCHAR(255),
            telefone VARCHAR(30),
            criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for ddl in [
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS codigo_usuario VARCHAR(50)",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS nome VARCHAR(255)",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS senha_hash TEXT",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS nivel VARCHAR(50)",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS regional VARCHAR(255)",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS ativo BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS email VARCHAR(255)",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS telefone VARCHAR(30)",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS supervisor_codigo VARCHAR(50)",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS gerente_codigo VARCHAR(50)",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    ]:
        cur.execute(ddl)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS saf_configuracoes_sistema (
            chave VARCHAR(100) PRIMARY KEY,
            valor TEXT,
            atualizado_por_codigo VARCHAR(50),
            atualizado_por_nome VARCHAR(255),
            atualizado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for ddl in [
        "ALTER TABLE saf_configuracoes_sistema ADD COLUMN IF NOT EXISTS chave VARCHAR(100)",
        "ALTER TABLE saf_configuracoes_sistema ADD COLUMN IF NOT EXISTS valor TEXT",
        "ALTER TABLE saf_configuracoes_sistema ADD COLUMN IF NOT EXISTS atualizado_por_codigo VARCHAR(50)",
        "ALTER TABLE saf_configuracoes_sistema ADD COLUMN IF NOT EXISTS atualizado_por_nome VARCHAR(255)",
        "ALTER TABLE saf_configuracoes_sistema ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
    ]:
        cur.execute(ddl)

    garantir_constraint_tipo_saf(cur)

    conn.commit()
    cur.close()
    conn.close()


def texto_limpo(v):
    if v is None:
        return None
    v = str(v).strip()
    return v or None


def numero_brasil_para_float(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace('R$', '').replace(' ', '')
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except Exception:
        return None


def data_iso_para_date(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except Exception:
        try:
            return pd.to_datetime(s, errors='coerce').date()
        except Exception:
            return None


def calcular_total_item_saf(
    tipo_saf: Optional[str],
    valor: Optional[float],
    despesas_financeiras: Optional[float],
    percentual_juros: Optional[float] = None,
    valor_juros: Optional[float] = None
) -> Optional[float]:
    tipo = (tipo_saf or '').strip().upper()
    valor_num = 0.0 if valor is None else float(valor)
    desp_num = 0.0 if despesas_financeiras is None else float(despesas_financeiras)
    juros_valor = float(valor_juros) if valor_juros is not None else 0.0
    if tipo == 'PRORROGAR_COM_JUROS' and juros_valor == 0.0 and percentual_juros is not None:
        juros_valor = round(valor_num * (float(percentual_juros) / 100.0), 2)
    if tipo in {'ALTERAR_PORTADOR_DEVOLUCAO', 'PRORROGAR_SEM_JUROS'}:
        return round(valor_num + desp_num, 2)
    if tipo == 'PRORROGAR_COM_JUROS':
        return round(valor_num + desp_num + juros_valor, 2)
    return None


def somar_totais_itens_saf(itens: list[dict]) -> float:
    total = 0.0
    for item in itens or []:
        valor = numero_brasil_para_float(item.get('valor')) or 0.0
        despesas = numero_brasil_para_float(item.get('despesas_financeiras')) or 0.0
        item_total = item.get('total')
        if item_total is None or item_total == '':
            item_total = calcular_total_item_saf(item.get('tipo_saf'), valor, despesas)
        try:
            total += float(item_total or 0)
        except Exception:
            pass
    return round(total, 2)


APPROVER_ROLES = ("supervisor", "gerente", "diretor", "supervisor_financeiro")


def usuario_logado_codigo() -> str:
    return str(session.get("codigo", "") or "")


def usuario_logado_nome() -> str:
    return str(session.get("nome", "") or "")


def usuario_logado_role() -> str:
    return str(session.get("role", "usuario") or "usuario")


def usuario_e_admin() -> bool:
    return usuario_logado_role() == "admin"


def usuario_e_aprovador() -> bool:
    return usuario_logado_role() in APPROVER_ROLES or usuario_e_admin()


def eh_tipo_titulos(tipo_saf: Optional[str]) -> bool:
    tipo = (tipo_saf or "").strip().upper()
    return tipo in {
        "TITULOS_DEVOLUCAO",
        "ALTERACAO_TITULOS",
        "ALTERACAO_TITULO",
        "ALTERACAO_DE_TITULOS",
        "TITULOS",
        "TITULO",
        "ALTERAR_TITULOS",
    }

def normalizar_status_saf(status: Optional[str]) -> str:
    status = (status or "").strip().upper()
    if not status or status in {"PENDENTE", "PENDENTE_SUPERVISOR"}:
        return "PENDENTE_SUPERVISOR"
    if status == "APROVADO_FINAL":
        return "FINALIZADO"
    return status


def proximo_status_apos_aprovacao(saf: dict) -> str:
    atual = normalizar_status_saf(saf.get("status"))
    if atual == "PENDENTE_SUPERVISOR":
        return "PENDENTE_GERENTE"
    if atual == "PENDENTE_GERENTE":
        return "PENDENTE_DIRETOR"
    if atual == "PENDENTE_DIRETOR":
        return "PENDENTE_EXECUCAO_ATENDENTE"
    if atual == "PENDENTE_SUPERVISOR_FINANCEIRO":
        return "PENDENTE_FINANCEIRO"
    if atual == "PENDENTE_FINANCEIRO":
        return "PENDENTE_AUTENTICACAO_ATENDENTE"
    if atual == "PENDENTE_EXECUCAO_ATENDENTE":
        return "PENDENTE_SUPERVISOR_FINANCEIRO"
    if atual == "PENDENTE_AUTENTICACAO_ATENDENTE":
        return "FINALIZADO"
    return atual

def pode_editar_saf(saf: dict) -> bool:
    if not saf:
        return False
    if usuario_e_admin():
        return True
    codigo = (usuario_logado_codigo() or "").strip().upper()
    criador = str(saf.get("criado_por_codigo") or "").strip().upper()
    return (
        bool(codigo)
        and criador == codigo
        and normalizar_status_saf(saf.get("status")) in {"PENDENTE_SUPERVISOR", "REPROVADO"}
    )

def pode_decidir_saf(saf: dict) -> bool:
    if not saf:
        return False
    if usuario_e_admin():
        return True
    role = usuario_logado_role()
    status = normalizar_status_saf(saf.get("status"))
    codigo = (usuario_logado_codigo() or "").strip().upper()
    criador = str(saf.get("criado_por_codigo") or "").strip().upper()
    if criador == codigo:
        return False
    if role == "supervisor":
        return status == "PENDENTE_SUPERVISOR" and saf_pertence_ao_supervisor(saf)
    if role == "gerente":
        return status == "PENDENTE_GERENTE"
    if role == "diretor":
        return status == "PENDENTE_DIRETOR"
    if role == "supervisor_financeiro":
        return status == "PENDENTE_SUPERVISOR_FINANCEIRO"
    return False

def pode_visualizar_saf(saf: dict) -> bool:
    if not saf:
        return False
    if usuario_e_admin():
        return True

    role = usuario_logado_role()
    codigo = (usuario_logado_codigo() or "").strip().upper()
    status = normalizar_status_saf(saf.get("status"))
    criador = str(saf.get("criado_por_codigo") or "").strip().upper()

    if role == "atendente":
        return criador == codigo

    if role == "supervisor":
        return criador == codigo or saf_pertence_ao_supervisor(saf)

    if role in {"gerente", "diretor"}:
        return True

    if role == "supervisor_financeiro":
        return True

    if role == "financeiro":
        return True

    return False

def pode_executar_saf(saf: dict) -> bool:
    if not saf:
        return False
    if usuario_e_admin():
        return True

    role = usuario_logado_role()
    codigo = (usuario_logado_codigo() or "").strip().upper()
    status = normalizar_status_saf(saf.get("status"))
    criador = str(saf.get("criado_por_codigo") or "").strip().upper()

    if role == "financeiro":
        return status == "PENDENTE_FINANCEIRO"

    if role == "atendente":
        return status == "PENDENTE_EXECUCAO_ATENDENTE" and criador == codigo

    return False

def pode_autenticar_saf(saf: dict) -> bool:
    if not saf:
        return False
    if usuario_e_admin():
        return True

    role = usuario_logado_role()
    codigo = (usuario_logado_codigo() or "").strip().upper()
    status = normalizar_status_saf(saf.get("status"))
    criador = str(saf.get("criado_por_codigo") or "").strip().upper()

    return (
        role == "atendente"
        and status == "PENDENTE_AUTENTICACAO_ATENDENTE"
        and criador == codigo
    )


def pode_excluir_anexo_saf(saf: dict, anexo: Optional[dict] = None) -> bool:
    if not saf:
        return False
    if usuario_e_admin():
        return True

    codigo = usuario_logado_codigo()
    if not codigo:
        return False

    # Regra do projeto: somente o criador da SAF pode editar/excluir anexos.
    return str(saf.get("criado_por_codigo") or "").strip().upper() == str(codigo).strip().upper()


def cor_status_badge(status: Optional[str]) -> str:
    st = normalizar_status_saf(status)
    cores = {
        "PENDENTE_SUPERVISOR": "#1565C0",              # azul
        "PENDENTE_GERENTE": "#6A1B9A",                 # roxo
        "PENDENTE_DIRETOR": "#000000",                 # verde petróleo
        "PENDENTE_SUPERVISOR_FINANCEIRO": "#455A64",   # cinza azulado
        "PENDENTE_EXECUCAO_ATENDENTE": "#3949AB",      # índigo
        "PENDENTE_FINANCEIRO": "#00838F",              # azul petróleo
        "PENDENTE_AUTENTICACAO_ATENDENTE": "#03D9FF",  # violeta
        "FINALIZADO": "#2E7D32",                       # verde
        "REPROVADO": "#F300B6",                        # marrom escuro
    }
    return cores.get(st, "#546E7A")


def status_badge_html(status: Optional[str]) -> str:
    st = normalizar_status_saf(status)
    labels = {
        "PENDENTE_SUPERVISOR": "Pendente Supervisor",
        "PENDENTE_GERENTE": "Pendente Gerência",
        "PENDENTE_DIRETOR": "Pendente Diretoria",
        "PENDENTE_SUPERVISOR_FINANCEIRO": "Pendente Supervisor Financeiro",
        "PENDENTE_EXECUCAO_ATENDENTE": "Pendente Execução Atendente",
        "PENDENTE_FINANCEIRO": "Pendente Financeiro 2",
        "PENDENTE_AUTENTICACAO_ATENDENTE": "Pendente Conferência Final Atendente",
        "FINALIZADO": "Finalizado",
        "REPROVADO": "Reprovado",
    }
    cor = cor_status_badge(st)
    label = labels.get(st, escape(st or "-"))
    return (
        f'<span class="badge" '
        f'style="background:{cor};color:#ffffff;border-color:{cor};">{label}</span>'
    )

def label_status_saf(status: Optional[str]) -> str:
    st = normalizar_status_saf(status)
    labels = {
        "PENDENTE_SUPERVISOR": "Pendente Supervisor",
        "PENDENTE_GERENTE": "Pendente Gerência",
        "PENDENTE_DIRETOR": "Pendente Diretoria",
        "PENDENTE_SUPERVISOR_FINANCEIRO": "Pendente Supervisor Financeiro",
        "PENDENTE_EXECUCAO_ATENDENTE": "Pendente Execução Atendente",
        "PENDENTE_FINANCEIRO": "Pendente Financeiro 2",
        "PENDENTE_AUTENTICACAO_ATENDENTE": "Pendente Conferência Final Atendente",
        "FINALIZADO": "Finalizado",
        "REPROVADO": "Reprovado",
    }
    return labels.get(st, st or "-")

def normalizar_prioridade_saf(prioridade: Optional[str]) -> str:
    p = (prioridade or '').strip().upper()
    return 'URGENTE' if p == 'URGENTE' else 'NORMAL'

def label_prioridade_saf(prioridade: Optional[str]) -> str:
    return 'Urgente' if normalizar_prioridade_saf(prioridade) == 'URGENTE' else 'Normal'

def badge_prioridade_saf(prioridade: Optional[str], atrasada: bool = False) -> str:
    if atrasada:
        return '<span class="badge" style="background:#B71C1C;color:#ffffff;border-color:#B71C1C;">Atrasada</span>'
    if normalizar_prioridade_saf(prioridade) == 'URGENTE':
        return '<span class="badge" style="background:#E65100;color:#ffffff;border-color:#E65100;">Urgente</span>'
    return '<span class="badge badge-neutral">Normal</span>'

def saf_esta_pendente(status: Optional[str]) -> bool:
    return normalizar_status_saf(status) not in {'FINALIZADO', 'REPROVADO'}

def saf_atrasada(saf: Optional[dict]) -> bool:
    if not saf or not saf_esta_pendente(saf.get('status')):
        return False
    data_ref = saf.get('data_solicitacao')
    if not data_ref:
        return False
    try:
        if hasattr(data_ref, 'date'):
            data_ref = data_ref.date() if not isinstance(data_ref, __import__('datetime').date) else data_ref
    except Exception:
        pass
    try:
        dt = pd.to_datetime(data_ref, errors='coerce')
        if pd.isna(dt):
            return False
        data_base = dt.date()
    except Exception:
        return False
    return (datetime.now().date() - data_base).days >= 2

def estilo_linha_saf(saf: dict) -> str:
    if not saf:
        return ''

    status = str(saf.get('status') or '').strip().upper()
    prioridade = str(saf.get('prioridade') or '').strip().upper()

    if status == 'FINALIZADO':
        return 'background-color:#51fc57 !important;'

    if saf_atrasada(saf):
        return 'background-color:#FFCDD2 !important;'

    if prioridade == 'URGENTE':
        return 'background-color:#ff8800 !important;'

    return ''

def fetchone_dict(cur):
    row = cur.fetchone()
    if row is None:
        return None
    return {desc[0]: row[idx] for idx, desc in enumerate(cur.description)}


def fetchall_dict(cur):
    rows = cur.fetchall()
    return [{desc[0]: row[idx] for idx, desc in enumerate(cur.description)} for row in rows]


def obter_saf(saf_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            id, tipo_saf, data_solicitacao, supervisor, codigo_representante, representante,
            atendente_codigo, atendente_nome, codigo_cliente, razao_social, ocorrencia_geral,
            prioridade, status, criado_por_codigo, criado_por_nome, perfil_criador,
            ultima_acao_aprovacao, ultima_observacao_aprovacao, ultimo_aprovador_codigo,
            ultimo_aprovador_nome, perfil_aprovador, data_aprovacao, criado_em, atualizado_em,
            executado_por_codigo, executado_por_nome, executado_por_perfil, data_execucao, observacao_execucao,
            autenticado_por_codigo, autenticado_por_nome, autenticado_por_perfil, data_autenticacao, observacao_autenticacao
        FROM saf_solicitacoes
        WHERE id = %s
    """, (saf_id,))
    registro = fetchone_dict(cur)
    cur.close()
    conn.close()
    if registro:
        registro["status"] = normalizar_status_saf(registro.get("status"))
    return registro


def listar_itens_saf(saf_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            id, saf_id, ordem, tipo_saf, pedido, dc, pares, valor, novo_dc, titulo,
            vencimento, novo_portador, despesas_financeiras, total, situacao, acao, ocorrencia_item
        FROM saf_itens
        WHERE saf_id = %s
        ORDER BY ordem, id
    """, (saf_id,))
    itens = fetchall_dict(cur)
    cur.close()
    conn.close()
    return itens


def listar_aprovacoes_saf(saf_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, saf_id, acao, observacao, usuario_codigo, usuario_nome, usuario_perfil, criado_em
        FROM saf_aprovacoes
        WHERE saf_id = %s
        ORDER BY criado_em DESC, id DESC
    """, (saf_id,))
    itens = fetchall_dict(cur)
    cur.close()
    conn.close()
    return itens


def listar_anexos_saf(saf_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            id, saf_id, nome_original, nome_salvo, caminho_arquivo, extensao,
            tamanho_bytes, mime_type, enviado_por_codigo, enviado_por_nome,
            enviado_por_perfil, criado_em
        FROM saf_anexos
        WHERE saf_id = %s
        ORDER BY criado_em DESC, id DESC
    """, (saf_id,))
    itens = fetchall_dict(cur)
    cur.close()
    conn.close()
    return itens


def obter_anexo_saf(anexo_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            id, saf_id, nome_original, nome_salvo, caminho_arquivo, extensao,
            tamanho_bytes, mime_type, enviado_por_codigo, enviado_por_nome,
            enviado_por_perfil, criado_em
        FROM saf_anexos
        WHERE id = %s
        LIMIT 1
    """, (anexo_id,))
    item = fetchone_dict(cur)
    cur.close()
    conn.close()
    return item


def listar_log_anexos_saf(saf_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            id, saf_id, anexo_id, acao, nome_arquivo, observacao,
            usuario_codigo, usuario_nome, usuario_perfil, criado_em
        FROM saf_anexos_log
        WHERE saf_id = %s
        ORDER BY criado_em DESC, id DESC
    """, (saf_id,))
    itens = fetchall_dict(cur)
    cur.close()
    conn.close()
    return itens


def registrar_log_anexo(cur, saf_id: int, anexo_id: Optional[int], acao: str, nome_arquivo: Optional[str], observacao: Optional[str] = None):
    cur.execute("""
        INSERT INTO saf_anexos_log (
            saf_id, anexo_id, acao, nome_arquivo, observacao,
            usuario_codigo, usuario_nome, usuario_perfil
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        saf_id,
        anexo_id,
        acao,
        nome_arquivo,
        observacao,
        usuario_logado_codigo(),
        usuario_logado_nome(),
        usuario_logado_role(),
    ))


def registrar_log_aprovacao(cur, saf_id: int, acao: str, observacao: Optional[str]):
    cur.execute("""
        INSERT INTO saf_aprovacoes (
            saf_id, acao, observacao, usuario_codigo, usuario_nome, usuario_perfil
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        saf_id,
        acao,
        observacao,
        usuario_logado_codigo(),
        usuario_logado_nome(),
        usuario_logado_role(),
    ))


def valor_input_texto(valor):
    return "" if valor is None else escape(str(valor))


def valor_input_data(valor):
    if not valor:
        return ""
    if hasattr(valor, "strftime"):
        return valor.strftime("%Y-%m-%d")
    return escape(str(valor))


def valor_display_data(valor):
    if not valor:
        return "-"
    if hasattr(valor, "strftime"):
        return valor.strftime("%d/%m/%Y")
    try:
        return pd.to_datetime(valor).strftime("%d/%m/%Y")
    except Exception:
        return str(valor)


def valor_display_numero(valor):
    if valor is None or valor == "":
        return ""
    try:
        return formatar_moeda(valor)
    except Exception:
        return str(valor)



def flash_messages_html() -> str:
    mensagens = get_flashed_messages(with_categories=True)
    if not mensagens:
        return ""
    blocos = []
    for categoria, mensagem in mensagens:
        cat = (categoria or "info").strip().lower()
        classe = "badge-warning"
        if cat in {"success", "ok"}:
            classe = "badge-success"
        elif cat in {"error", "danger"}:
            classe = "badge-danger"
        blocos.append(f'<div class="panel" style="border-left:6px solid #f97316; margin-bottom:16px;"><div class="badge {classe}">{escape(cat.title())}</div><p style="margin-top:10px;">{escape(str(mensagem))}</p></div>')
    return ''.join(blocos)


def formatar_duracao_horas(valor_horas: Optional[float]) -> str:
    if valor_horas is None:
        return "-"
    try:
        total_min = int(round(float(valor_horas) * 60))
    except Exception:
        return "-"
    dias, resto = divmod(total_min, 1440)
    horas, minutos = divmod(resto, 60)
    partes = []
    if dias:
        partes.append(f"{dias}d")
    if horas:
        partes.append(f"{horas}h")
    if minutos or not partes:
        partes.append(f"{minutos}min")
    return ' '.join(partes)


def normalizar_whatsapp(numero: Optional[str]) -> Optional[str]:
    if not numero:
        return None
    cfg = obter_config_runtime_notificacoes()
    default_country_code = cfg.get('WHATSAPP_DEFAULT_COUNTRY_CODE') or WHATSAPP_DEFAULT_COUNTRY_CODE or '+55'
    s = ''.join(ch for ch in str(numero) if ch.isdigit() or ch == '+')
    if not s:
        return None
    if s.startswith('00'):
        s = '+' + s[2:]
    if not s.startswith('+'):
        digits = ''.join(ch for ch in s if ch.isdigit())
        if digits.startswith('55'):
            s = '+' + digits
        else:
            s = str(default_country_code) + digits
    return s

def normalizar_email(email: Optional[str]) -> Optional[str]:
    email = texto_limpo(email)
    if not email:
        return None
    email = str(email).strip()
    return email.lower() if '@' in email else email


def email_provider_ready() -> bool:
    cfg = obter_config_runtime_notificacoes()
    return bool(cfg.get('EMAIL_HOST') and cfg.get('EMAIL_PORT') and cfg.get('EMAIL_FROM') and cfg.get('EMAIL_USER') and cfg.get('EMAIL_PASS'))


def enviar_email_smtp(destino_email: str, assunto: str, mensagem: str):
    destino_email = normalizar_email(destino_email)
    if not destino_email:
        return False, 'E-mail inválido.', None
    cfg = obter_config_runtime_notificacoes()
    email_host = cfg.get('EMAIL_HOST')
    email_port = cfg.get('EMAIL_PORT')
    email_from = cfg.get('EMAIL_FROM')
    email_from_name = cfg.get('EMAIL_FROM_NAME')
    email_reply_to = cfg.get('EMAIL_REPLY_TO')
    email_user = cfg.get('EMAIL_USER')
    email_pass = cfg.get('EMAIL_PASS')
    email_use_tls = cfg.get('EMAIL_USE_TLS')
    email_use_ssl = cfg.get('EMAIL_USE_SSL')
    email_timeout = cfg.get('EMAIL_TIMEOUT')
    if not email_provider_ready():
        return False, 'SMTP não configurado.', None

    corpo_html = f"""
    <html>
      <body style="font-family:Arial,Helvetica,sans-serif;background:#fff7ed;color:#111827;padding:24px;">
        <div style="max-width:700px;margin:0 auto;background:#ffffff;border:1px solid #fdba74;border-radius:18px;padding:24px;">
          <h2 style="margin:0 0 16px 0;color:#ea580c;">Atualização SAF</h2>
          <p style="margin:0;white-space:pre-line;line-height:1.6;">{escape(mensagem).replace('\n', '<br>')}</p>
        </div>
      </body>
    </html>
    """

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = assunto
        msg['From'] = formataddr((str(email_from_name or '').strip(), email_from)) if str(email_from_name or '').strip() else email_from
        msg['To'] = destino_email
        if str(email_reply_to or '').strip():
            msg['Reply-To'] = str(email_reply_to).strip()
        msg.attach(MIMEText(mensagem, 'plain', 'utf-8'))
        msg.attach(MIMEText(corpo_html, 'html', 'utf-8'))

        smtp_cls = smtplib.SMTP_SSL if email_use_ssl else smtplib.SMTP
        with smtp_cls(email_host, email_port, timeout=email_timeout) as server:
            if email_use_tls and not email_use_ssl:
                server.starttls()
            if email_user:
                server.login(email_user, email_pass)
            server.sendmail(email_from, [destino_email], msg.as_string())

        print(f"[SAF][EMAIL] Enviado para {destino_email} | assunto={assunto}")
        return True, 'E-mail enviado com sucesso.', None
    except Exception as e:
        print(f"[SAF][EMAIL] Erro ao enviar para {destino_email}: {e}")
        return False, str(e), None


def obter_email_usuario(codigo_usuario: Optional[str]) -> Optional[str]:
    garantir_colunas_vinculo_usuarios()
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario:
        return None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT email
              FROM usuarios
             WHERE UPPER(COALESCE(codigo_usuario, '')) = UPPER(%s)
             LIMIT 1
        """, (codigo_usuario,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return normalizar_email(row[0]) if row and row[0] else None
    except Exception:
        return None


def obter_telefone_usuario(codigo_usuario: Optional[str]) -> Optional[str]:
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario:
        return None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT telefone
              FROM usuarios
             WHERE UPPER(COALESCE(codigo_usuario, '')) = UPPER(%s)
             LIMIT 1
        """, (codigo_usuario,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return normalizar_whatsapp(row[0]) if row and row[0] else None
    except Exception:
        return None


def listar_usuarios_ativos_por_nivel(nivel: Optional[str], regional: Optional[str] = None):
    nivel = texto_limpo(nivel)
    regional = texto_limpo(regional)
    if not nivel:
        return []
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if regional:
            cur.execute("""
                SELECT id, codigo_usuario, nome, nivel, regional, ativo, email, telefone
                  FROM usuarios
                 WHERE ativo = TRUE
                   AND LOWER(COALESCE(nivel, '')) = LOWER(%s)
                   AND UPPER(COALESCE(regional, '')) = UPPER(%s)
                 ORDER BY nome, codigo_usuario
            """, (nivel, regional))
        else:
            cur.execute("""
                SELECT id, codigo_usuario, nome, nivel, regional, ativo, email, telefone
                  FROM usuarios
                 WHERE ativo = TRUE
                   AND LOWER(COALESCE(nivel, '')) = LOWER(%s)
                 ORDER BY nome, codigo_usuario
            """, (nivel,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows or []
    except Exception:
        return []


def enriquecer_destinatario_com_contatos(dest: dict) -> dict:
    dest = dict(dest or {})
    perfil = (dest.get('perfil') or '').strip().lower()
    dest['telefone_whatsapp'] = normalizar_whatsapp(dest.get('telefone_whatsapp'))
    dest['email'] = normalizar_email(dest.get('email'))

    telefone_cadastro = obter_telefone_usuario(dest.get('codigo_usuario'))
    email_cadastro = obter_email_usuario(dest.get('codigo_usuario'))

    if perfil in {'gerente', 'diretor'}:
        if telefone_cadastro:
            dest['telefone_whatsapp'] = telefone_cadastro
    else:
        if email_cadastro:
            dest['email'] = email_cadastro

    return dest


def canal_notificacao_por_perfil(perfil: Optional[str]) -> str:
    perfil = (perfil or '').strip().lower()
    if perfil in {'gerente', 'diretor'}:
        return 'whatsapp'
    return 'email'


def montar_assunto_email_saf(saf: dict, evento: str, novo_status: Optional[str] = None) -> str:
    status_destino = label_status_saf(novo_status or saf.get('status'))
    return f"SAF {saf.get('id')} | {evento.replace('_', ' ').title()} | {status_destino}"


def garantir_destinatarios_whatsapp_padrao():
    for perfil, cfg in WHATSAPP_DESTINOS_FIXOS.items():
        telefone = normalizar_whatsapp(cfg.get('telefone_whatsapp')) or str(cfg.get('telefone_whatsapp') or '')
        try:
            salvar_destinatario_whatsapp(
                codigo_usuario=str(cfg.get('codigo_usuario') or '').upper(),
                nome_usuario=cfg.get('nome_usuario') or perfil.title(),
                perfil=perfil,
                telefone_whatsapp=telefone,
                email=normalizar_email(cfg.get('email')),
                ativo=True,
                recebe_criacao=True,
                recebe_aprovacao=True,
                recebe_reprovacao=True,
                recebe_observacao=True,
            )
        except Exception as e:
            print(f"[SAF][WhatsApp] Erro ao garantir destinatário padrão {perfil}: {e}")


def destinatarios_fixos_por_perfis(perfis):
    itens = []
    vistos = set()
    for perfil in perfis or []:
        cfg = WHATSAPP_DESTINOS_FIXOS.get((perfil or '').strip().lower())
        if not cfg:
            continue
        telefone = normalizar_whatsapp(cfg.get('telefone_whatsapp'))
        key = (str(cfg.get('codigo_usuario') or ''), str(telefone or ''))
        if not telefone or key in vistos:
            continue
        vistos.add(key)
        itens.append({
            'codigo_usuario': str(cfg.get('codigo_usuario') or '').upper(),
            'nome_usuario': cfg.get('nome_usuario') or perfil.title(),
            'perfil': (perfil or '').strip().lower(),
            'telefone_whatsapp': telefone,
            'email': normalizar_email(cfg.get('email')),
            'ativo': True,
            'recebe_criacao': True,
            'recebe_aprovacao': True,
            'recebe_reprovacao': True,
            'recebe_observacao': True,
        })
    return itens



def resumo_config_notificacoes() -> dict:
    cfg = obter_config_runtime_notificacoes()
    sid = cfg.get('TWILIO_ACCOUNT_SID') or ''
    return {
        "whatsapp_provider": cfg.get('WHATSAPP_PROVIDER') or 'twilio',
        "twilio_configurado": bool(sid and cfg.get('TWILIO_AUTH_TOKEN') and cfg.get('TWILIO_WHATSAPP_FROM')),
        "twilio_sid_final": (sid[-6:] if sid else ""),
        "email_configurado": bool(cfg.get('EMAIL_HOST') and cfg.get('EMAIL_PORT') and cfg.get('EMAIL_USER') and cfg.get('EMAIL_PASS') and cfg.get('EMAIL_FROM')),
        "email_host": cfg.get('EMAIL_HOST') or '',
        "email_from": cfg.get('EMAIL_FROM') or '',
        "email_from_name": cfg.get('EMAIL_FROM_NAME') or '',
    }

def whatsapp_provider_ready() -> bool:
    cfg = obter_config_runtime_notificacoes()
    return bool((cfg.get('WHATSAPP_PROVIDER') or 'twilio') == 'twilio' and cfg.get('TWILIO_ACCOUNT_SID') and cfg.get('TWILIO_AUTH_TOKEN') and cfg.get('TWILIO_WHATSAPP_FROM'))


def twilio_from_formatado() -> str:
    cfg = obter_config_runtime_notificacoes()
    from_number = cfg.get('TWILIO_WHATSAPP_FROM') or TWILIO_WHATSAPP_FROM or ''
    return from_number if str(from_number).startswith('whatsapp:') else f'whatsapp:{from_number}'


def twilio_endpoint_messages() -> str:
    cfg = obter_config_runtime_notificacoes()
    sid = cfg.get('TWILIO_ACCOUNT_SID') or ''
    return f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"


def twilio_endpoint_message_sid(message_sid: str) -> str:
    cfg = obter_config_runtime_notificacoes()
    sid = cfg.get('TWILIO_ACCOUNT_SID') or ''
    return f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages/{message_sid}.json"


def safe_json_loads(texto: Optional[str]):
    if not texto:
        return None
    try:
        return json.loads(texto)
    except Exception:
        return None


def extrair_json_resposta(resp) -> dict:
    try:
        data = resp.json()
        return data if isinstance(data, dict) else {'raw_json': data}
    except Exception:
        try:
            return {'raw_text': resp.text}
        except Exception:
            return {}


def consultar_status_mensagem_twilio(message_sid: str):
    message_sid = texto_limpo(message_sid)
    if not message_sid:
        return False, {'erro': 'SID da mensagem não informado.'}
    if not whatsapp_provider_ready():
        return False, {'erro': 'Twilio não configurado.'}

    try:
        resp = requests.get(
            twilio_endpoint_message_sid(message_sid),
            auth=((obter_config_runtime_notificacoes().get('TWILIO_ACCOUNT_SID') or ''), (obter_config_runtime_notificacoes().get('TWILIO_AUTH_TOKEN') or '')),
            timeout=(obter_config_runtime_notificacoes().get('WHATSAPP_TIMEOUT') or WHATSAPP_TIMEOUT),
        )
        data = extrair_json_resposta(resp)
        print(f"[SAF][WhatsApp][STATUS] SID {message_sid} | HTTP {resp.status_code}")
        print(f"[SAF][WhatsApp][STATUS] Resposta: {data}")
        if 200 <= resp.status_code < 300:
            return True, data
        return False, data
    except Exception as e:
        print(f"[SAF][WhatsApp][STATUS] Exceção ao consultar SID {message_sid}: {e}")
        return False, {'erro': str(e)}


def acompanhar_status_mensagem_twilio(message_sid: str, tentativas: Optional[int] = None, espera_segundos: Optional[float] = None):
    cfg = obter_config_runtime_notificacoes()
    tentativas = max(1, int(tentativas if tentativas is not None else (cfg.get('TWILIO_STATUS_POLL_ATTEMPTS') or TWILIO_STATUS_POLL_ATTEMPTS)))
    espera_segundos = float(espera_segundos if espera_segundos is not None else (cfg.get('TWILIO_STATUS_POLL_SECONDS') or TWILIO_STATUS_POLL_SECONDS))

    ultimo_ok = False
    ultimo_payload = {}

    for tentativa in range(1, tentativas + 1):
        if tentativa > 1 and espera_segundos > 0:
            time.sleep(espera_segundos)

        ok, payload = consultar_status_mensagem_twilio(message_sid)
        ultimo_ok = ok
        ultimo_payload = payload if isinstance(payload, dict) else {'retorno': payload}
        status = str(ultimo_payload.get('status') or '').strip().lower()
        error_code = ultimo_payload.get('error_code')
        error_message = ultimo_payload.get('error_message')

        print(
            f"[SAF][WhatsApp][DIAG] Tentativa {tentativa}/{tentativas} | "
            f"SID {message_sid} | status={status or '-'} | error_code={error_code} | error_message={error_message}"
        )

        if status in {'delivered', 'sent', 'read'}:
            break
        if status in {'failed', 'undelivered', 'canceled'}:
            break

    return ultimo_ok, ultimo_payload


def registrar_log_notificacao(saf_id: Optional[int], evento: str, destino: dict, status_envio: str, mensagem: str, resposta: Optional[str] = None, provedor: Optional[str] = None):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO saf_notificacoes_log (
                saf_id, evento, destino_codigo_usuario, destino_nome_usuario, destino_perfil,
                telefone_whatsapp, email_destino, provedor, status_envio, mensagem, resposta_provedor
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            saf_id,
            evento,
            destino.get('codigo_usuario'),
            destino.get('nome_usuario'),
            destino.get('perfil'),
            destino.get('telefone_whatsapp'),
            destino.get('email'),
            provedor or WHATSAPP_PROVIDER,
            status_envio,
            mensagem,
            resposta,
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass


def criar_notificacao_interna(codigo_usuario: Optional[str], titulo: str, mensagem: str, saf_id: Optional[int] = None, evento: Optional[str] = None):
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario:
        return
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO saf_notificacoes_internas (codigo_usuario, titulo, mensagem, saf_id, evento)
            VALUES (%s, %s, %s, %s, %s)
        """, (codigo_usuario, texto_limpo(titulo), mensagem, saf_id, texto_limpo(evento)))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[SAF][NotificacaoInterna] Erro ao criar notificação para {codigo_usuario}: {e}")


def contar_notificacoes_internas_nao_lidas(codigo_usuario: Optional[str]) -> int:
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario and not usuario_e_admin():
        return 0
    try:
        conn = get_conn()
        cur = conn.cursor()
        if usuario_e_admin():
            cur.execute("""
                SELECT COUNT(*)
                  FROM saf_notificacoes_internas
                 WHERE COALESCE(lida, FALSE) = FALSE
            """)
        else:
            cur.execute("""
                SELECT COUNT(*)
                  FROM saf_notificacoes_internas
                 WHERE UPPER(COALESCE(codigo_usuario, '')) = UPPER(%s)
                   AND COALESCE(lida, FALSE) = FALSE
            """, (codigo_usuario,))
        total = cur.fetchone()[0] or 0
        cur.close()
        conn.close()
        return int(total)
    except Exception:
        return 0


def listar_notificacoes_internas_usuario(codigo_usuario: Optional[str], limite: int = 200):
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario and not usuario_e_admin():
        return []
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if usuario_e_admin():
            cur.execute("""
                SELECT id, codigo_usuario, titulo, mensagem, saf_id, evento, lida, criada_em, lida_em
                  FROM saf_notificacoes_internas
                 ORDER BY criada_em DESC, id DESC
                 LIMIT %s
            """, (limite,))
        else:
            cur.execute("""
                SELECT id, codigo_usuario, titulo, mensagem, saf_id, evento, lida, criada_em, lida_em
                  FROM saf_notificacoes_internas
                 WHERE UPPER(COALESCE(codigo_usuario, '')) = UPPER(%s)
                 ORDER BY criada_em DESC, id DESC
                 LIMIT %s
            """, (codigo_usuario, limite))
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return rows
    except Exception:
        return []


def marcar_notificacao_interna_lida(notificacao_id: int, codigo_usuario: Optional[str]) -> bool:
    codigo_usuario = texto_limpo(codigo_usuario)
    if not notificacao_id or (not codigo_usuario and not usuario_e_admin()):
        return False
    try:
        conn = get_conn()
        cur = conn.cursor()
        if usuario_e_admin():
            cur.execute("""
                UPDATE saf_notificacoes_internas
                   SET lida = TRUE,
                       lida_em = CURRENT_TIMESTAMP
                 WHERE id = %s
            """, (notificacao_id,))
        else:
            cur.execute("""
                UPDATE saf_notificacoes_internas
                   SET lida = TRUE,
                       lida_em = CURRENT_TIMESTAMP
                 WHERE id = %s
                   AND UPPER(COALESCE(codigo_usuario, '')) = UPPER(%s)
            """, (notificacao_id, codigo_usuario))
        ok = cur.rowcount > 0
        conn.commit()
        cur.close()
        conn.close()
        return ok
    except Exception:
        return False


def marcar_todas_notificacoes_internas_lidas(codigo_usuario: Optional[str]) -> int:
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario and not usuario_e_admin():
        return 0
    try:
        conn = get_conn()
        cur = conn.cursor()
        if usuario_e_admin():
            cur.execute("""
                UPDATE saf_notificacoes_internas
                   SET lida = TRUE,
                       lida_em = CURRENT_TIMESTAMP
                 WHERE COALESCE(lida, FALSE) = FALSE
            """)
        else:
            cur.execute("""
                UPDATE saf_notificacoes_internas
                   SET lida = TRUE,
                       lida_em = CURRENT_TIMESTAMP
                 WHERE UPPER(COALESCE(codigo_usuario, '')) = UPPER(%s)
                   AND COALESCE(lida, FALSE) = FALSE
            """, (codigo_usuario,))
        total = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return total
    except Exception:
        return 0


def montar_titulo_notificacao_interna(saf: dict, evento: str, novo_status: Optional[str] = None) -> str:
    status_destino = label_status_saf(novo_status or saf.get('status'))
    if evento == 'SAF_CRIADA':
        return f"Nova SAF {saf.get('id')}"
    if evento == 'SAF_APROVADA':
        return f"SAF {saf.get('id')} aprovada"
    if evento == 'SAF_REPROVADA':
        return f"SAF {saf.get('id')} reprovada"
    if evento == 'SAF_OBSERVADA':
        return f"SAF {saf.get('id')} com observação"
    if evento in {'SAF_EXECUTADA', 'SAF_AUTENTICADA', 'SAF_FINALIZADA'}:
        return f"SAF {saf.get('id')} atualizada"
    return f"SAF {saf.get('id')} | {status_destino}"


def listar_destinatarios_whatsapp(perfil: Optional[str] = None):
    conn = get_conn()
    cur = conn.cursor()
    if perfil:
        cur.execute("""
            SELECT id, codigo_usuario, nome_usuario, perfil, telefone_whatsapp, email, ativo,
                   recebe_criacao, recebe_aprovacao, recebe_reprovacao, recebe_observacao,
                   criado_em, atualizado_em
              FROM saf_notificacoes_usuarios
             WHERE ativo = TRUE AND perfil = %s
             ORDER BY ativo DESC, atualizado_em DESC, criado_em DESC, id DESC
        """, (perfil,))
    else:
        cur.execute("""
            SELECT id, codigo_usuario, nome_usuario, perfil, telefone_whatsapp, email, ativo,
                   recebe_criacao, recebe_aprovacao, recebe_reprovacao, recebe_observacao,
                   criado_em, atualizado_em
              FROM saf_notificacoes_usuarios
             ORDER BY ativo DESC, atualizado_em DESC, criado_em DESC, id DESC
        """)
    itens = fetchall_dict(cur)
    cur.close()
    conn.close()
    return itens


def listar_destinatarios_whatsapp_reais(perfil: Optional[str] = None):
    itens = []
    try:
        for reg in listar_destinatarios_whatsapp(perfil):
            codigo = str(reg.get('codigo_usuario') or '').strip().upper()
            if codigo.startswith('FIXO_'):
                continue
            itens.append(reg)
    except Exception:
        itens = []
    return itens


def deduplicar_destinatarios(destinos: list[dict]) -> list[dict]:
    unicos = []
    vistos = set()
    for dest in destinos or []:
        canal = canal_notificacao_por_perfil(dest.get('perfil'))
        codigo = str(dest.get('codigo_usuario') or '').strip().upper()
        telefone = normalizar_whatsapp(dest.get('telefone_whatsapp'))
        email = normalizar_email(dest.get('email'))
        contato_chave = telefone if canal == 'whatsapp' else email
        chave_base = (codigo or str(dest.get('nome_usuario') or '').strip().upper(), canal)
        if not contato_chave or chave_base in vistos:
            continue
        vistos.add(chave_base)
        dest['telefone_whatsapp'] = telefone
        dest['email'] = email
        unicos.append(dest)
    return unicos


def obter_destinatarios_por_codigo(codigo_usuario: Optional[str]):
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario:
        return []
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, codigo_usuario, nome_usuario, perfil, telefone_whatsapp, email, ativo,
               recebe_criacao, recebe_aprovacao, recebe_reprovacao, recebe_observacao,
               criado_em, atualizado_em
          FROM saf_notificacoes_usuarios
         WHERE ativo = TRUE AND UPPER(codigo_usuario) = %s
         ORDER BY nome_usuario, telefone_whatsapp
    """, (codigo_usuario.upper(),))
    itens = fetchall_dict(cur)
    cur.close()
    conn.close()
    return itens


def salvar_destinatario_whatsapp(codigo_usuario: str, nome_usuario: str, perfil: str, telefone_whatsapp: str,
                                 email: Optional[str], ativo: bool, recebe_criacao: bool, recebe_aprovacao: bool,
                                 recebe_reprovacao: bool, recebe_observacao: bool):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO saf_notificacoes_usuarios (
            codigo_usuario, nome_usuario, perfil, telefone_whatsapp, email, ativo,
            recebe_criacao, recebe_aprovacao, recebe_reprovacao, recebe_observacao, atualizado_em
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (codigo_usuario, telefone_whatsapp)
        DO UPDATE SET
            nome_usuario = EXCLUDED.nome_usuario,
            perfil = EXCLUDED.perfil,
            email = EXCLUDED.email,
            ativo = EXCLUDED.ativo,
            recebe_criacao = EXCLUDED.recebe_criacao,
            recebe_aprovacao = EXCLUDED.recebe_aprovacao,
            recebe_reprovacao = EXCLUDED.recebe_reprovacao,
            recebe_observacao = EXCLUDED.recebe_observacao,
            atualizado_em = CURRENT_TIMESTAMP
    """, (codigo_usuario, nome_usuario, perfil, telefone_whatsapp, email, ativo, recebe_criacao, recebe_aprovacao, recebe_reprovacao, recebe_observacao))
    conn.commit()
    cur.close()
    conn.close()

def excluir_destinatario_whatsapp(dest_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM saf_notificacoes_usuarios WHERE id = %s", (dest_id,))
    conn.commit()
    cur.close()
    conn.close()


def limpar_variavel_twilio(valor: Optional[str]) -> str:
    texto = '' if valor is None else str(valor)
    texto = texto.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
    while '    ' in texto:
        texto = texto.replace('    ', '   ')
    texto = ' '.join(texto.split())
    return texto.strip() or '-'


def montar_variaveis_template_whatsapp(saf: dict, evento: str, novo_status: Optional[str] = None, observacao: Optional[str] = None):
    status_destino = label_status_saf(novo_status or saf.get('status'))
    campo_1 = f"SAF {saf.get('id')} - {evento.replace('_', ' ').title()}"
    campo_2 = f"{status_destino} | {saf.get('tipo_saf') or '-'} | Cliente {saf.get('codigo_cliente') or '-'}"
    obs = texto_limpo(observacao)
    if obs:
        campo_2 = f"{campo_2} | {obs[:60]}"
    return {
        "1": limpar_variavel_twilio(campo_1),
        "2": limpar_variavel_twilio(campo_2),
    }


def _executar_envio_whatsapp_twilio(payload: dict):
    resp = requests.post(
        twilio_endpoint_messages(),
        data=payload,
        auth=((obter_config_runtime_notificacoes().get('TWILIO_ACCOUNT_SID') or ''), (obter_config_runtime_notificacoes().get('TWILIO_AUTH_TOKEN') or '')),
        timeout=(obter_config_runtime_notificacoes().get('WHATSAPP_TIMEOUT') or WHATSAPP_TIMEOUT)
    )
    resposta_texto = None
    try:
        resposta_texto = resp.text
    except Exception:
        resposta_texto = None
    resposta_json = extrair_json_resposta(resp)
    return resp, resposta_texto, resposta_json


def _resultado_envio_whatsapp(telefone: str, payload: dict, resp, resposta_texto, resposta_json):
    message_sid = texto_limpo((resposta_json or {}).get('sid'))
    status_inicial = texto_limpo((resposta_json or {}).get('status'))
    error_code = (resposta_json or {}).get('error_code')
    error_message = (resposta_json or {}).get('error_message')

    print(f"[SAF][WhatsApp] Payload para {telefone}: {payload}")
    print(f"[SAF][WhatsApp] Status: {resp.status_code}")
    print(f"[SAF][WhatsApp] Resposta: {resposta_texto}")
    if message_sid:
        print(f"[SAF][WhatsApp] Message SID: {message_sid}")
    if status_inicial:
        print(f"[SAF][WhatsApp] Status inicial Twilio: {status_inicial}")
    if error_code or error_message:
        print(f"[SAF][WhatsApp] Error code: {error_code} | Error message: {error_message}")

    retorno_log = dict(resposta_json) if isinstance(resposta_json, dict) else {'raw_text': resposta_texto}

    if 200 <= resp.status_code < 300:
        if message_sid:
            ok_diag, diag = acompanhar_status_mensagem_twilio(message_sid)
            if isinstance(diag, dict) and diag:
                retorno_log['consulta_status'] = diag
                status_final = str(diag.get('status') or '').strip().lower()
                if status_final in {'failed', 'undelivered', 'canceled'}:
                    msg_erro = diag.get('error_message') or f'Mensagem com status {status_final}.'
                    print(f"[SAF][WhatsApp] Falha final para {telefone}: SID {message_sid} | {msg_erro}")
                    return False, msg_erro, json.dumps(retorno_log, ensure_ascii=False)
                if status_final in {'delivered', 'sent', 'read'}:
                    print(f"[SAF][WhatsApp] Entrega confirmada para {telefone}: SID {message_sid} | status={status_final}")
                    return True, f'Mensagem enviada com status {status_final}.', json.dumps(retorno_log, ensure_ascii=False)
                print(f"[SAF][WhatsApp] Mensagem aceita pela Twilio para {telefone}: SID {message_sid} | status={status_final or status_inicial or '-'}")
                return True, f"Mensagem aceita pela Twilio com status {status_final or status_inicial or 'queued'}.", json.dumps(retorno_log, ensure_ascii=False)

        print(f"[SAF][WhatsApp] Enviado para {telefone}: HTTP {resp.status_code}")
        return True, 'Mensagem enviada com sucesso.', json.dumps(retorno_log, ensure_ascii=False)

    print(f"[SAF][WhatsApp] Falha para {telefone}: HTTP {resp.status_code} -> {resposta_texto}")
    return False, f'Falha Twilio HTTP {resp.status_code}.', json.dumps(retorno_log, ensure_ascii=False)


def enviar_whatsapp_twilio(telefone: str, mensagem: str, content_variables: Optional[dict] = None):
    telefone = normalizar_whatsapp(telefone)
    if not telefone:
        return False, 'Telefone inválido.', None
    if not whatsapp_provider_ready():
        return False, 'Twilio não configurado.', None

    body_texto = limpar_variavel_twilio(mensagem)
    payload_body = {
        'To': f'whatsapp:{telefone}',
        'From': twilio_from_formatado(),
        'Body': body_texto,
    }

    try:
        resp, resposta_texto, resposta_json = _executar_envio_whatsapp_twilio(payload_body)
        return _resultado_envio_whatsapp(telefone, payload_body, resp, resposta_texto, resposta_json)
    except Exception as e:
        print(f"[SAF][WhatsApp] Exceção para {telefone}: {e}")
        return False, str(e), None


def montar_mensagem_whatsapp_saf(saf: dict, evento: str, novo_status: Optional[str] = None, observacao: Optional[str] = None) -> str:
    status_destino = label_status_saf(novo_status or saf.get('status'))
    obs = texto_limpo(observacao)
    partes = [
        f"SAF {saf.get('id')}",
        f"Tipo {saf.get('tipo_saf') or '-'}",
        f"Cliente {saf.get('codigo_cliente') or '-'} - {saf.get('razao_social') or '-'}",
        f"Status {status_destino}",
        f"Evento {evento.replace('_', ' ').title()}",
    ]
    if obs:
        partes.append(f"Obs {obs}")
    partes.append('Acesse o painel SAF para tratar a solicitação.')
    return ' | '.join(partes)



def perfis_destino_por_status(status: Optional[str]):
    st = normalizar_status_saf(status)
    mapa = {
        'PENDENTE_SUPERVISOR': ['supervisor'],
        'PENDENTE_GERENTE': ['gerente'],
        'PENDENTE_DIRETOR': ['diretor'],
        'PENDENTE_SUPERVISOR_FINANCEIRO': ['supervisor_financeiro'],
        'PENDENTE_FINANCEIRO': ['financeiro'],
        'PENDENTE_EXECUCAO_ATENDENTE': ['atendente'],
        'PENDENTE_AUTENTICACAO_ATENDENTE': ['atendente'],
    }
    return mapa.get(st, [])


def obter_usuario_vinculo_notificacao(codigo_usuario: Optional[str]):
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario:
        return None
    try:
        return obter_usuario_admin(codigo_usuario)
    except Exception:
        try:
            conn = get_conn()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT
                    u.codigo_usuario,
                    u.nome,
                    u.nivel,
                    u.regional,
                    u.ativo,
                    u.email,
                    u.telefone,
                    u.supervisor_codigo,
                    u.gerente_codigo
                  FROM usuarios u
                 WHERE UPPER(COALESCE(u.codigo_usuario, '')) = UPPER(%s)
                 LIMIT 1
            """, (codigo_usuario,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            return row
        except Exception:
            return None


def montar_destinatario_usuario_vinculado(usuario: Optional[dict], chave_preferencia: Optional[str] = None):
    if not usuario:
        return None
    dest = {
        'codigo_usuario': usuario.get('codigo_usuario'),
        'nome_usuario': usuario.get('nome'),
        'perfil': usuario.get('nivel'),
        'telefone_whatsapp': usuario.get('telefone'),
        'email': usuario.get('email'),
        'ativo': bool(usuario.get('ativo', True)),
        'recebe_criacao': True,
        'recebe_aprovacao': True,
        'recebe_reprovacao': True,
        'recebe_observacao': True,
    }
    dest = enriquecer_destinatario_com_contatos(dest)
    if chave_preferencia and not dest.get(chave_preferencia, True):
        return None
    return dest


def obter_hierarquia_notificacao_saf(saf: dict):
    criador_codigo = texto_limpo(saf.get('criado_por_codigo') or saf.get('atendente_codigo'))
    criador = obter_usuario_vinculo_notificacao(criador_codigo) if criador_codigo else None

    supervisor = None
    gerente = None

    if criador:
        nivel_criador = str(criador.get('nivel') or '').strip().lower()
        if nivel_criador == 'atendente':
            supervisor_codigo = texto_limpo(criador.get('supervisor_codigo'))
            if supervisor_codigo:
                supervisor = obter_usuario_vinculo_notificacao(supervisor_codigo)
            gerente_codigo = texto_limpo((supervisor or {}).get('gerente_codigo'))
            if gerente_codigo:
                gerente = obter_usuario_vinculo_notificacao(gerente_codigo)
        elif nivel_criador == 'supervisor':
            supervisor = criador
            gerente_codigo = texto_limpo(criador.get('gerente_codigo'))
            if gerente_codigo:
                gerente = obter_usuario_vinculo_notificacao(gerente_codigo)
        elif nivel_criador == 'gerente':
            gerente = criador

    return {
        'criador': criador,
        'supervisor': supervisor,
        'gerente': gerente,
    }



def obter_gerente_vinculado_ao_supervisor(codigo_supervisor: Optional[str]):
    codigo_supervisor = texto_limpo(codigo_supervisor)
    if not codigo_supervisor:
        return None
    supervisor = obter_usuario_vinculo_notificacao(codigo_supervisor)
    if not supervisor:
        return None
    gerente_codigo = texto_limpo(supervisor.get('gerente_codigo'))
    if not gerente_codigo:
        return None
    return obter_usuario_vinculo_notificacao(gerente_codigo)


def obter_supervisor_vinculado_ao_atendente(codigo_atendente: Optional[str]):
    codigo_atendente = texto_limpo(codigo_atendente)
    if not codigo_atendente:
        return None
    atendente = obter_usuario_vinculo_notificacao(codigo_atendente)
    if not atendente:
        return None
    supervisor_codigo = texto_limpo(atendente.get('supervisor_codigo'))
    if not supervisor_codigo:
        return None
    return obter_usuario_vinculo_notificacao(supervisor_codigo)

def enviar_notificacoes_whatsapp(saf: dict, evento: str, novo_status: Optional[str] = None, observacao: Optional[str] = None):
    if not saf:
        return

    try:
        garantir_destinatarios_whatsapp_padrao()
    except Exception as e:
        print(f"[SAF][Notificacao] Erro ao garantir destinatários padrão: {e}")

    destinos = []
    status_ref = normalizar_status_saf(novo_status or saf.get('status'))
    hierarquia = obter_hierarquia_notificacao_saf(saf)

    if evento == 'SAF_CRIADA' and status_ref == 'PENDENTE_SUPERVISOR':
        supervisor_forcado = obter_supervisor_vinculado_ao_atendente(saf.get('criado_por_codigo') or saf.get('atendente_codigo'))
        if supervisor_forcado:
            hierarquia['supervisor'] = supervisor_forcado
            print(f"[SAF][Notificacao] SAF {saf.get('id')} criada por atendente -> supervisor vinculado {supervisor_forcado.get('codigo_usuario')}")
        else:
            print(f"[SAF][Notificacao] SAF {saf.get('id')} criada por atendente sem supervisor vinculado válido.")

    if evento == 'SAF_APROVADA' and status_ref == 'PENDENTE_GERENTE' and usuario_logado_role() == 'supervisor':
        gerente_forcado = obter_gerente_vinculado_ao_supervisor(usuario_logado_codigo())
        if gerente_forcado:
            hierarquia['gerente'] = gerente_forcado
            print(f"[SAF][Notificacao] Supervisor {usuario_logado_codigo()} aprovou SAF {saf.get('id')} -> gerente vinculado {gerente_forcado.get('codigo_usuario')}")
        else:
            print(f"[SAF][Notificacao] Supervisor {usuario_logado_codigo()} sem gerente vinculado válido para SAF {saf.get('id')}.")

    def add_destino_unico(dest: Optional[dict]):
        if dest:
            destinos.append(dest)

    def add_destino_vinculado_por_papel(papel: str, chave_preferencia: Optional[str] = None):
        usuario = hierarquia.get(papel)
        dest = montar_destinatario_usuario_vinculado(usuario, chave_preferencia=chave_preferencia)
        if dest:
            destinos.append(dest)

    def add_destinos_por_perfil(perfis, chave_preferencia=None):
        for perfil in perfis or []:
            registros = []
            try:
                registros.extend(listar_destinatarios_whatsapp_reais(perfil))
            except Exception as e:
                print(f"[SAF][Notificacao] Erro ao listar destinatários reais do perfil {perfil}: {e}")

            regional_alvo = None
            if perfil == 'supervisor':
                regional_alvo = texto_limpo(saf.get('supervisor'))

            for usuario in listar_usuarios_ativos_por_nivel(perfil, regional_alvo):
                registros.append({
                    'codigo_usuario': usuario.get('codigo_usuario'),
                    'nome_usuario': usuario.get('nome'),
                    'perfil': usuario.get('nivel'),
                    'telefone_whatsapp': usuario.get('telefone'),
                    'email': usuario.get('email'),
                    'ativo': True,
                    'recebe_criacao': True,
                    'recebe_aprovacao': True,
                    'recebe_reprovacao': True,
                    'recebe_observacao': True,
                })

            # Só usa destinos FIXOS quando realmente não houver nenhum usuário real para a etapa.
            if not registros:
                registros = destinatarios_fixos_por_perfis([perfil])

            for dest in deduplicar_destinatarios(registros):
                dest = enriquecer_destinatario_com_contatos(dest)
                if chave_preferencia and not dest.get(chave_preferencia, True):
                    continue
                destinos.append(dest)

    if evento in {'SAF_CRIADA', 'SAF_ATUALIZADA'}:
        # Prioriza vínculo real: atendente -> supervisor / supervisor -> gerente
        if status_ref == 'PENDENTE_SUPERVISOR':
            add_destino_vinculado_por_papel('supervisor', 'recebe_criacao')
        elif status_ref == 'PENDENTE_GERENTE':
            add_destino_vinculado_por_papel('gerente', 'recebe_criacao')
        else:
            perfis_iniciais = perfis_destino_por_status(status_ref)
            add_destinos_por_perfil(perfis_iniciais or ['supervisor'], 'recebe_criacao')

    elif evento == 'SAF_APROVADA':
        if status_ref == 'PENDENTE_GERENTE':
            add_destino_vinculado_por_papel('gerente', 'recebe_aprovacao')
        elif status_ref == 'PENDENTE_SUPERVISOR':
            add_destino_vinculado_por_papel('supervisor', 'recebe_aprovacao')
        elif status_ref in {'PENDENTE_DIRETOR', 'PENDENTE_SUPERVISOR_FINANCEIRO', 'PENDENTE_FINANCEIRO'}:
            add_destinos_por_perfil(perfis_destino_por_status(status_ref), 'recebe_aprovacao')
        elif status_ref in {'PENDENTE_EXECUCAO_ATENDENTE', 'PENDENTE_AUTENTICACAO_ATENDENTE', 'FINALIZADO'}:
            add_destino_vinculado_por_papel('criador', 'recebe_aprovacao')

    elif evento == 'SAF_REPROVADA':
        add_destino_vinculado_por_papel('criador', 'recebe_reprovacao')

    elif evento == 'SAF_OBSERVADA':
        add_destino_vinculado_por_papel('criador', 'recebe_observacao')

    elif evento == 'SAF_EXECUTADA':
        if status_ref in {'PENDENTE_SUPERVISOR_FINANCEIRO', 'PENDENTE_FINANCEIRO'}:
            add_destinos_por_perfil(perfis_destino_por_status(status_ref), 'recebe_aprovacao')
        else:
            add_destino_vinculado_por_papel('criador', 'recebe_aprovacao')

    elif evento in {'SAF_AUTENTICADA', 'SAF_FINALIZADA'}:
        add_destino_vinculado_por_papel('criador', 'recebe_aprovacao')

    unicos = deduplicar_destinatarios(destinos)

    mensagem = montar_mensagem_whatsapp_saf(saf, evento, novo_status=novo_status, observacao=observacao)
    assunto_email = montar_assunto_email_saf(saf, evento, novo_status=novo_status)
    variaveis_template = montar_variaveis_template_whatsapp(saf, evento, novo_status=novo_status, observacao=observacao)
    if not unicos:
        registrar_log_notificacao(saf.get('id'), evento, {'codigo_usuario': None, 'nome_usuario': None, 'perfil': None, 'telefone_whatsapp': None, 'email': None}, 'SEM_DESTINATARIO', mensagem, 'Nenhum destinatário configurado.')
        print(f"[SAF][Notificacao] Nenhum destinatário configurado para SAF {saf.get('id')} / {evento}")
        return

    for dest in unicos:
        try:
            criar_notificacao_interna(
                dest.get('codigo_usuario'),
                montar_titulo_notificacao_interna(saf, evento, novo_status=novo_status),
                mensagem,
                saf_id=saf.get('id'),
                evento=evento,
            )
        except Exception as e:
            print(f"[SAF][NotificacaoInterna] Erro ao registrar notificação interna da SAF {saf.get('id')}: {e}")

        canal = canal_notificacao_por_perfil(dest.get('perfil'))
        if canal == 'whatsapp':
            telefone = dest.get('telefone_whatsapp')
            print(f"[SAF][Notificacao] Enviando WhatsApp evento={evento} saf={saf.get('id')} destino={dest.get('codigo_usuario')} perfil={dest.get('perfil')} telefone={telefone}")
            ok, status_msg, resposta = enviar_whatsapp_twilio(telefone, mensagem, content_variables=variaveis_template)

            status_envio = 'ENVIADO_WPP'
            status_msg_l = (status_msg or '').lower()
            resposta_json = safe_json_loads(resposta) if isinstance(resposta, str) else None
            consulta_status = resposta_json.get('consulta_status') if isinstance(resposta_json, dict) else None
            status_twilio = str(((consulta_status or {}).get('status') if isinstance(consulta_status, dict) else None) or '').strip().lower()

            if not ok:
                status_envio = 'PENDENTE_CONFIG_WPP' if 'configurado' in status_msg_l else 'ERRO_WPP'
            elif status_twilio in {'queued', 'accepted', 'sending', 'scheduled'}:
                status_envio = 'ACEITO_TWILIO'
            elif status_twilio in {'failed', 'undelivered', 'canceled'}:
                status_envio = 'ERRO_WPP'

            registrar_log_notificacao(saf.get('id'), evento, dest, status_envio, mensagem, resposta or status_msg, 'twilio')
        else:
            email = dest.get('email')
            ok, status_msg, resposta = enviar_email_smtp(email, assunto_email, mensagem)
            status_envio = 'ENVIADO_EMAIL' if ok else ('PENDENTE_CONFIG_EMAIL' if 'configurado' in (status_msg or '').lower() else 'ERRO_EMAIL')
            registrar_log_notificacao(saf.get('id'), evento, dest, status_envio, mensagem, resposta or status_msg, 'smtp')


def construir_html_tabela(headers, rows):
    if not rows:
        colspan = max(len(headers), 1)
        return f'<table class="saf-table"><thead><tr>{"".join(f"<th>{escape(str(h))}</th>" for h in headers)}</tr></thead><tbody><tr><td colspan="{colspan}">Nenhum dado encontrado.</td></tr></tbody></table>'
    thead = ''.join(f'<th>{escape(str(h))}</th>' for h in headers)
    body = []
    for row in rows:
        body.append('<tr>' + ''.join(f'<td>{cell}</td>' for cell in row) + '</tr>')
    return f'<table class="saf-table"><thead><tr>{thead}</tr></thead><tbody>{"".join(body)}</tbody></table>'




def obter_codigos_atendentes_vinculados(codigo_supervisor: Optional[str]) -> list[str]:
    codigo_supervisor = texto_limpo(codigo_supervisor)
    if not codigo_supervisor:
        return []
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT codigo_usuario
              FROM usuarios
             WHERE COALESCE(ativo, TRUE) = TRUE
               AND LOWER(COALESCE(nivel, '')) = 'atendente'
               AND UPPER(COALESCE(supervisor_codigo, '')) = UPPER(%s)
             ORDER BY codigo_usuario
        """, (codigo_supervisor,))
        rows = [str(r[0] or '').strip().upper() for r in cur.fetchall() if r and r[0]]
        cur.close()
        conn.close()
        return [r for r in rows if r]
    except Exception:
        return []


def montar_filtro_dashboard_safs(alias: str = 's'):
    role = usuario_logado_role()
    codigo = (usuario_logado_codigo() or '').strip().upper()
    regionais = lista_regionais_usuario()

    if role in {'admin', 'diretor', 'gerente'}:
        return '1=1', []

    if role == 'atendente':
        return f"UPPER(COALESCE({alias}.criado_por_codigo, '')) = UPPER(%s)", [codigo]

    if role == 'supervisor':
        condicoes = []
        params = []
        if codigo:
            condicoes.append(f"UPPER(COALESCE({alias}.criado_por_codigo, '')) = UPPER(%s)")
            params.append(codigo)

            atendentes = obter_codigos_atendentes_vinculados(codigo)
            if atendentes:
                placeholders = ', '.join(['%s'] * len(atendentes))
                condicoes.append(f"UPPER(COALESCE({alias}.criado_por_codigo, '')) IN ({placeholders})")
                params.extend([c.upper() for c in atendentes])

        if regionais:
            placeholders = ', '.join(['%s'] * len(regionais))
            condicoes.append(f"UPPER(COALESCE({alias}.supervisor, '')) IN ({placeholders})")
            params.extend([r.upper() for r in regionais])

        if not condicoes:
            return '1=0', []
        return '(' + ' OR '.join(condicoes) + ')', params

    return '1=1', []

def montar_dashboard_metricas():
    metricas = {
        'cards': {'total': 0, 'pendentes': 0, 'finalizadas': 0, 'reprovadas': 0, 'minhas': 0},
        'status_rows': [],
        'tipo_rows': [],
        'usuario_rows': [],
        'duracoes': {'primeira_decisao': None, 'finalizacao': None, 'ultima_etapa': None},
        'notif_rows': [],
    }
    try:
        conn = get_conn()
        cur = conn.cursor()
        filtro_safs, params_safs = montar_filtro_dashboard_safs('s')

        cur.execute(f"SELECT COUNT(*) FROM saf_solicitacoes s WHERE {filtro_safs}", tuple(params_safs))
        metricas['cards']['total'] = cur.fetchone()[0]

        cur.execute(
            f"SELECT COUNT(*) FROM saf_solicitacoes s WHERE {filtro_safs} AND COALESCE(s.status, 'PENDENTE_SUPERVISOR') NOT IN ('FINALIZADO', 'REPROVADO')",
            tuple(params_safs)
        )
        metricas['cards']['pendentes'] = cur.fetchone()[0]

        cur.execute(
            f"SELECT COUNT(*) FROM saf_solicitacoes s WHERE {filtro_safs} AND COALESCE(s.status, 'PENDENTE_SUPERVISOR') = 'REPROVADO'",
            tuple(params_safs)
        )
        metricas['cards']['reprovadas'] = cur.fetchone()[0]

        cur.execute(
            f"SELECT COUNT(*) FROM saf_solicitacoes s WHERE {filtro_safs} AND COALESCE(s.status, 'PENDENTE_SUPERVISOR') = 'FINALIZADO'",
            tuple(params_safs)
        )
        metricas['cards']['finalizadas'] = cur.fetchone()[0]

        cur.execute(
            f"SELECT COUNT(*) FROM saf_solicitacoes s WHERE {filtro_safs} AND UPPER(COALESCE(s.criado_por_codigo, '')) = UPPER(%s)",
            tuple(params_safs) + (usuario_logado_codigo(),)
        )
        metricas['cards']['minhas'] = cur.fetchone()[0]

        cur.execute(f"""
            SELECT COALESCE(s.status, 'PENDENTE_SUPERVISOR') AS status, COUNT(*) AS total
              FROM saf_solicitacoes s
             WHERE {filtro_safs}
             GROUP BY COALESCE(s.status, 'PENDENTE_SUPERVISOR')
             ORDER BY total DESC, status
        """, tuple(params_safs))
        metricas['status_rows'] = fetchall_dict(cur)

        cur.execute(f"""
            SELECT COALESCE(s.tipo_saf, 'SEM TIPO') AS tipo_saf, COUNT(*) AS total
              FROM saf_solicitacoes s
             WHERE {filtro_safs}
             GROUP BY COALESCE(s.tipo_saf, 'SEM TIPO')
             ORDER BY total DESC, tipo_saf
             LIMIT 15
        """, tuple(params_safs))
        metricas['tipo_rows'] = fetchall_dict(cur)

        cur.execute(f"""
            SELECT s.criado_por_codigo,
                   COALESCE(s.criado_por_nome, s.criado_por_codigo, 'Sem usuário') AS nome,
                   COUNT(*) AS qtd_criadas
              FROM saf_solicitacoes s
             WHERE {filtro_safs}
             GROUP BY s.criado_por_codigo, COALESCE(s.criado_por_nome, s.criado_por_codigo, 'Sem usuário')
             ORDER BY qtd_criadas DESC, nome
             LIMIT 15
        """, tuple(params_safs))
        criadas = fetchall_dict(cur)

        cur.execute(f"""
            SELECT a.usuario_codigo,
                   COALESCE(a.usuario_nome, a.usuario_codigo, 'Sem usuário') AS nome,
                   COUNT(*) FILTER (WHERE a.acao = 'APROVAR') AS qtd_aprovacoes,
                   COUNT(*) FILTER (WHERE a.acao = 'REPROVAR') AS qtd_reprovacoes,
                   COUNT(*) FILTER (WHERE a.acao = 'OBSERVAR') AS qtd_observacoes
              FROM saf_aprovacoes a
              JOIN saf_solicitacoes s ON s.id = a.saf_id
             WHERE {filtro_safs}
             GROUP BY a.usuario_codigo, COALESCE(a.usuario_nome, a.usuario_codigo, 'Sem usuário')
             ORDER BY qtd_aprovacoes DESC, nome
             LIMIT 15
        """, tuple(params_safs))
        acoes = fetchall_dict(cur)
        mapa_acoes = {str(r.get('usuario_codigo') or ''): r for r in acoes}
        usuarios_rows = []
        for r in criadas:
            a = mapa_acoes.get(str(r.get('criado_por_codigo') or ''), {})
            usuarios_rows.append({
                'nome': r.get('nome'),
                'codigo': r.get('criado_por_codigo') or '-',
                'qtd_criadas': r.get('qtd_criadas') or 0,
                'qtd_aprovacoes': a.get('qtd_aprovacoes') or 0,
                'qtd_reprovacoes': a.get('qtd_reprovacoes') or 0,
                'qtd_observacoes': a.get('qtd_observacoes') or 0,
            })
        metricas['usuario_rows'] = usuarios_rows

        cur.execute(f"""
            SELECT s.id, s.criado_em,
                   (SELECT MIN(a.criado_em) FROM saf_aprovacoes a WHERE a.saf_id = s.id AND a.acao IN ('APROVAR','REPROVAR','OBSERVAR')) AS primeira_decisao_em,
                   COALESCE(s.data_autenticacao, s.data_execucao, CASE WHEN s.status = 'FINALIZADO' THEN s.atualizado_em END) AS finalizacao_em,
                   s.data_aprovacao
              FROM saf_solicitacoes s
             WHERE {filtro_safs}
        """, tuple(params_safs))
        registros = fetchall_dict(cur)

        cur.execute(f"""
            SELECT n.evento, n.status_envio, COUNT(*) AS total
              FROM saf_notificacoes_log n
              JOIN saf_solicitacoes s ON s.id = n.saf_id
             WHERE {filtro_safs}
             GROUP BY n.evento, n.status_envio
             ORDER BY n.evento, n.status_envio
        """, tuple(params_safs))
        metricas['notif_rows'] = fetchall_dict(cur)
        cur.close()
        conn.close()

        primeira_decisao_h = []
        finalizacao_h = []
        ultima_etapa_h = []
        for r in registros:
            criado = r.get('criado_em')
            primeira = r.get('primeira_decisao_em')
            final = r.get('finalizacao_em')
            ultima_aprov = r.get('data_aprovacao')
            if criado and primeira:
                primeira_decisao_h.append((primeira - criado).total_seconds() / 3600)
            if criado and final:
                finalizacao_h.append((final - criado).total_seconds() / 3600)
            if ultima_aprov and final and final >= ultima_aprov:
                ultima_etapa_h.append((final - ultima_aprov).total_seconds() / 3600)
        if primeira_decisao_h:
            metricas['duracoes']['primeira_decisao'] = sum(primeira_decisao_h) / len(primeira_decisao_h)
        if finalizacao_h:
            metricas['duracoes']['finalizacao'] = sum(finalizacao_h) / len(finalizacao_h)
        if ultima_etapa_h:
            metricas['duracoes']['ultima_etapa'] = sum(ultima_etapa_h) / len(ultima_etapa_h)
    except Exception:
        pass
    return metricas

# =========================
# CONTROLE IMPORTACAO
# =========================
def garantir_tabela_controle_importacao():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS controle_importacao_arquivos (
            id SERIAL PRIMARY KEY,
            nome_arquivo VARCHAR(100) UNIQUE NOT NULL,
            caminho_arquivo TEXT NOT NULL,
            ultima_modificacao TIMESTAMP,
            tamanho_arquivo BIGINT,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def obter_info_arquivo(path: str):
    if not os.path.exists(path):
        return None

    stat = os.stat(path)
    return {
        "mtime": datetime.fromtimestamp(stat.st_mtime),
        "size": stat.st_size,
    }


def precisa_sincronizar(nome_arquivo: str, caminho_arquivo: str) -> bool:
    info = obter_info_arquivo(caminho_arquivo)
    if info is None:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT ultima_modificacao, tamanho_arquivo
        FROM controle_importacao_arquivos
        WHERE nome_arquivo = %s
    """, (nome_arquivo,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return True

    ultima_mod_bd, tamanho_bd = row
    return ultima_mod_bd != info["mtime"] or int(tamanho_bd or 0) != int(info["size"])


def registrar_sincronizacao(nome_arquivo: str, caminho_arquivo: str):
    info = obter_info_arquivo(caminho_arquivo)
    if info is None:
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO controle_importacao_arquivos
        (nome_arquivo, caminho_arquivo, ultima_modificacao, tamanho_arquivo, atualizado_em)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (nome_arquivo)
        DO UPDATE SET
            caminho_arquivo = EXCLUDED.caminho_arquivo,
            ultima_modificacao = EXCLUDED.ultima_modificacao,
            tamanho_arquivo = EXCLUDED.tamanho_arquivo,
            atualizado_em = CURRENT_TIMESTAMP
    """, (
        nome_arquivo,
        caminho_arquivo,
        info["mtime"],
        info["size"],
    ))
    conn.commit()
    cur.close()
    conn.close()


def garantir_tabela_cache_titulos_detalhado():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cache_titulos_detalhado (
            id SERIAL PRIMARY KEY,
            nota_fiscal VARCHAR(50),
            titulo VARCHAR(100),
            cfop VARCHAR(20),
            situacao VARCHAR(100),
            vencimento DATE,
            valor NUMERIC(18,2),
            codigo_banco VARCHAR(20),
            banco VARCHAR(120),
            cod_portador VARCHAR(20),
            portador VARCHAR(120),
            data_cliente DATE,
            data_faturamento DATE,
            data_saida DATE,
            data_expedicao DATE,
            origem TEXT,
            criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for ddl in [
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS nota_fiscal VARCHAR(50)",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS titulo VARCHAR(100)",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS cfop VARCHAR(20)",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS situacao VARCHAR(100)",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS vencimento DATE",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS valor NUMERIC(18,2)",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS codigo_banco VARCHAR(20)",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS banco VARCHAR(120)",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS cod_portador VARCHAR(20)",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS portador VARCHAR(120)",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS data_cliente DATE",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS data_faturamento DATE",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS data_saida DATE",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS data_expedicao DATE",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS origem TEXT",
        "ALTER TABLE cache_titulos_detalhado ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    ]:
        cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()


# =========================
# IMPORTACAO CACHE
# =========================
def importar_clientes_cache():
    if not os.path.exists(ARQ_CLIENTES):
        print(f"[SAF] CLIENTES.xlsx não encontrado: {ARQ_CLIENTES}")
        return

    print("[SAF] Sincronizando CLIENTES.xlsx...")
    df = ler_excel_inteligente(ARQ_CLIENTES)

    col_codigo_cliente = achar_coluna(df, ["codigo_cliente", "codigo_do_cliente", "cod_cliente", "codigo_cliente_"])
    col_razao_social = achar_coluna(df, ["razao_social", "razao", "cliente", "nome_cliente"])
    col_codigo_rep = achar_coluna(df, ["codigo_representante_carteira", "codigo_representante", "cod_representante"])
    col_representante = achar_coluna(df, ["representante_carteira", "representante", "nome_representante"])
    col_supervisor = achar_coluna(df, ["supervisor"])
    col_cidade = achar_coluna(df, ["cidade", "municipio"])
    col_uf = achar_coluna(df, ["uf", "estado"])

    if not col_codigo_cliente or not col_razao_social:
        raise ValueError("CLIENTES.xlsx sem colunas mínimas de Código Cliente / Razão Social.")

    registros = []
    for _, row in df.iterrows():
        codigo_cliente = valor_texto(row.get(col_codigo_cliente))
        razao_social = valor_texto(row.get(col_razao_social))

        if not codigo_cliente and not razao_social:
            continue

        registros.append((
            codigo_cliente,
            razao_social,
            valor_texto(row.get(col_codigo_rep)) if col_codigo_rep else None,
            valor_texto(row.get(col_representante)) if col_representante else None,
            valor_texto(row.get(col_supervisor)) if col_supervisor else None,
            valor_texto(row.get(col_cidade)) if col_cidade else None,
            valor_texto(row.get(col_uf)) if col_uf else None,
            ARQ_CLIENTES,
        ))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE cache_clientes RESTART IDENTITY;")
    execute_batch(cur, """
        INSERT INTO cache_clientes
        (
            codigo_cliente,
            razao_social,
            codigo_representante,
            representante,
            supervisor,
            cidade,
            uf,
            origem
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, registros, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()

    registrar_sincronizacao("CLIENTES", ARQ_CLIENTES)
    print(f"[SAF] CLIENTES sincronizados: {len(registros)}")


def importar_pedidos_cache():
    if not os.path.exists(ARQ_PEDIDOS):
        print(f"[SAF] PEDIDOS.xlsx não encontrado: {ARQ_PEDIDOS}")
        return

    print("[SAF] Sincronizando PEDIDOS.xlsx...")
    df = ler_excel_inteligente(ARQ_PEDIDOS)

    col_codigo_cliente = achar_coluna(df, ["codigo_cliente", "codigo_do_cliente", "cod_cliente"])
    col_numero_pedido = achar_coluna(df, ["numero_pedido", "pedido", "num_pedido"])
    col_razao_social = achar_coluna(df, ["razao_social", "razao", "cliente", "nome_cliente"])
    col_codigo_rep = achar_coluna(df, ["codigo_representante_carteira", "codigo_representante", "cod_representante"])
    col_representante = achar_coluna(df, ["representante_carteira", "representante", "nome_representante"])
    col_supervisor = achar_coluna(df, ["supervisor"])
    col_dc = achar_coluna(df, ["data_entrega", "data_de_entrega", "data_entrega_prevista", "dt_entrega", "dc", "data_pedido", "data_do_pedido", "data"])
    col_pares = achar_coluna(df, ["qtd_venda", "pares", "quantidade", "qtd"])
    col_valor = achar_coluna(df, ["vlr_venda", "valor", "valor_venda"])

    if not col_codigo_cliente or not col_numero_pedido:
        raise ValueError("PEDIDOS.xlsx sem colunas mínimas de Código Cliente / Número Pedido.")

    registros = []
    for _, row in df.iterrows():
        codigo_cliente = valor_texto(row.get(col_codigo_cliente))
        numero_pedido = valor_texto(row.get(col_numero_pedido))

        if not codigo_cliente and not numero_pedido:
            continue

        registros.append((
            codigo_cliente,
            numero_pedido,
            valor_texto(row.get(col_razao_social)) if col_razao_social else None,
            valor_texto(row.get(col_codigo_rep)) if col_codigo_rep else None,
            valor_texto(row.get(col_representante)) if col_representante else None,
            valor_texto(row.get(col_supervisor)) if col_supervisor else None,
            valor_data(row.get(col_dc)) if col_dc else None,
            valor_numerico(row.get(col_pares)) if col_pares else None,
            valor_numerico(row.get(col_valor)) if col_valor else None,
            ARQ_PEDIDOS,
        ))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE cache_pedidos RESTART IDENTITY;")
    execute_batch(cur, """
        INSERT INTO cache_pedidos
        (
            codigo_cliente,
            numero_pedido,
            razao_social,
            codigo_representante,
            representante,
            supervisor,
            dc,
            pares,
            valor,
            origem
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, registros, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()

    registrar_sincronizacao("PEDIDOS", ARQ_PEDIDOS)
    print(f"[SAF] PEDIDOS sincronizados: {len(registros)}")


def importar_titulos_cache():
    if not os.path.exists(ARQ_TITULOS):
        print(f"[SAF] TITULOS.xlsx não encontrado: {ARQ_TITULOS}")
        return

    print("[SAF] Sincronizando TITULOS.xlsx...")
    df = ler_excel_inteligente(ARQ_TITULOS)

    col_codigo_cliente = achar_coluna(df, ["codigo_cliente", "codigo_do_cliente", "cod_cliente"])
    col_titulo = achar_coluna(df, ["documento", "titulo", "numero_titulo"])
    col_vencimento = achar_coluna(df, ["data_vencimento", "vencimento"])
    col_valor = achar_coluna(df, ["vlr_atrasado", "vlr_credito", "vlr_debito", "vlr_pago", "valor"])

    if not col_codigo_cliente or not col_titulo:
        raise ValueError("TITULOS.xlsx sem colunas mínimas de Código Cliente / Título.")

    registros = []
    for _, row in df.iterrows():
        codigo_cliente = valor_texto(row.get(col_codigo_cliente))
        titulo = valor_texto(row.get(col_titulo))

        if not codigo_cliente and not titulo:
            continue

        registros.append((
            codigo_cliente,
            titulo,
            valor_data(row.get(col_vencimento)) if col_vencimento else None,
            valor_numerico(row.get(col_valor)) if col_valor else None,
            ARQ_TITULOS,
        ))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE cache_titulos RESTART IDENTITY;")
    execute_batch(cur, """
        INSERT INTO cache_titulos
        (
            codigo_cliente,
            titulo,
            vencimento,
            valor,
            origem
        )
        VALUES (%s, %s, %s, %s, %s)
    """, registros, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()

    registrar_sincronizacao("TITULOS", ARQ_TITULOS)
    print(f"[SAF] TITULOS sincronizados: {len(registros)}")


def importar_titulos2_cache():
    if not os.path.exists(ARQ_TITULOS2):
        print(f"[SAF] TITULOS2.xlsx não encontrado: {ARQ_TITULOS2}")
        return

    print("[SAF] Sincronizando TITULOS2.xlsx...")
    print("[SAF] Caminho:", ARQ_TITULOS2)

    df = pd.read_excel(ARQ_TITULOS2, dtype=object)
    print("[SAF] Colunas originais:", list(df.columns))
    print("[SAF] Total linhas lidas:", len(df))
    print(df.head(3))

    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df.rename(columns={
        "NUMNFV": "nota_fiscal",
        "NUMTIT": "titulo",
        "TNSPRO": "cfop",
        "SITTIT": "situacao",
        "DATVENC": "vencimento",
        "VLRORI": "valor",
        "CODCRT": "codigo_banco",
        "DESBAN": "banco",
        "CODPOR": "cod_portador",
        "DESPOR": "portador",
        "DATCLI": "data_cliente",
        "DATFAT": "data_faturamento",
        "DATSAI": "data_saida",
        "DATEXP": "data_expedicao",
    })

    print("[SAF] Colunas após rename:", list(df.columns))

    registros = []
    for _, row in df.iterrows():
        nota_fiscal = valor_texto(row.get("nota_fiscal"))
        titulo = valor_texto(row.get("titulo"))
        if not nota_fiscal and not titulo:
            continue

        registros.append((
            nota_fiscal,
            titulo,
            valor_texto(row.get("cfop")),
            valor_texto(row.get("situacao")),
            valor_data(row.get("vencimento")),
            valor_numerico(row.get("valor")),
            valor_texto(row.get("codigo_banco")),
            valor_texto(row.get("banco")),
            valor_texto(row.get("cod_portador")),
            valor_texto(row.get("portador")),
            valor_data(row.get("data_cliente")),
            valor_data(row.get("data_faturamento")),
            valor_data(row.get("data_saida")),
            valor_data(row.get("data_expedicao")),
            ARQ_TITULOS2,
        ))

    print("[SAF] Registros montados para inserir:", len(registros))
    if registros:
        print("[SAF] Primeiro registro:", registros[0])

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE cache_titulos_detalhado RESTART IDENTITY;")
    execute_batch(cur, """
        INSERT INTO cache_titulos_detalhado
        (
            nota_fiscal,
            titulo,
            cfop,
            situacao,
            vencimento,
            valor,
            codigo_banco,
            banco,
            cod_portador,
            portador,
            data_cliente,
            data_faturamento,
            data_saida,
            data_expedicao,
            origem
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, registros, page_size=1000)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM cache_titulos_detalhado")
    print("[SAF] Total gravado no banco:", cur.fetchone()[0])

    cur.close()
    conn.close()

    registrar_sincronizacao("TITULOS2", ARQ_TITULOS2)
    print(f"[SAF] TITULOS2 sincronizados: {len(registros)}")




def sincronizar_bases_automaticamente():
    """
    Sincroniza as bases locais em cache. Mantém a aplicação resiliente:
    se uma base falhar, registra no terminal e segue com as demais.
    """
    tarefas = [
        ('CLIENTES', importar_clientes_cache),
        ('PEDIDOS', importar_pedidos_cache),
        ('TITULOS', importar_titulos_cache),
    ]

    try:
        if 'importar_titulos_detalhado_cache' in globals() and callable(globals().get('importar_titulos_detalhado_cache')):
            tarefas.append(('TITULOS2', globals()['importar_titulos_detalhado_cache']))
    except Exception:
        pass

    for nome, func in tarefas:
        try:
            func()
        except Exception as e:
            print(f"[SAF] Erro ao sincronizar base {nome}: {e}")

@app.before_request
def startup_sync():
    global BASES_JA_VERIFICADAS

    if BASES_JA_VERIFICADAS:
        return

    try:
        garantir_tabelas_saf()
        garantir_destinatarios_whatsapp_padrao()
        sincronizar_bases_automaticamente()
        BASES_JA_VERIFICADAS = True
    except Exception as e:
        BASES_JA_VERIFICADAS = False
        print(f"[SAF] Erro ao sincronizar bases: {repr(e)}")
        raise


# =========================
# CONSULTA CLIENTE
# =========================
def buscar_cliente_cache(codigo_cliente: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            codigo_cliente,
            razao_social,
            codigo_representante,
            representante,
            supervisor,
            cidade,
            uf
        FROM cache_clientes
        WHERE codigo_cliente = %s
        LIMIT 1
    """, (codigo_cliente,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "codigo_cliente": row[0] or "",
        "razao_social": row[1] or "",
        "codigo_representante": row[2] or "",
        "representante": row[3] or "",
        "supervisor": row[4] or "",
        "cidade": row[5] or "",
        "uf": row[6] or "",
    }


def formatar_data_input(valor):
    if not valor:
        return ""
    if isinstance(valor, datetime):
        return valor.strftime("%Y-%m-%d")
    return str(valor)


def buscar_pedido_cache(codigo_cliente: str, numero_pedido: str):
    codigo_cliente = (codigo_cliente or "").strip()
    numero_pedido = (numero_pedido or "").strip()
    if not codigo_cliente or not numero_pedido:
        return None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            numero_pedido,
            dc,
            pares,
            valor
        FROM cache_pedidos
        WHERE codigo_cliente = %s
          AND TRIM(COALESCE(numero_pedido, '')) = %s
        ORDER BY id DESC
        LIMIT 1
    """, (codigo_cliente, numero_pedido))
    row = cur.fetchone()

    if not row:
        cur.execute("""
            SELECT
                numero_pedido,
                dc,
                pares,
                valor
            FROM cache_pedidos
            WHERE codigo_cliente = %s
              AND numero_pedido ILIKE %s
            ORDER BY id DESC
            LIMIT 1
        """, (codigo_cliente, f"%{numero_pedido}%"))
        row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "pedido": row[0] or "",
        "dc": formatar_data_input(row[1]),
        "dc_display": row[1].strftime("%d/%m/%Y") if row[1] else "",
        "pares": "" if row[2] is None else str(int(row[2]) if float(row[2]).is_integer() else row[2]),
        "valor": "" if row[3] is None else formatar_moeda(row[3]),
    }


def buscar_titulo_cache(codigo_cliente: str, titulo: str):
    codigo_cliente = (codigo_cliente or "").strip()
    titulo = (titulo or "").strip()
    if not codigo_cliente or not titulo:
        return None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            titulo,
            vencimento,
            valor
        FROM cache_titulos
        WHERE codigo_cliente = %s
          AND TRIM(COALESCE(titulo, '')) = %s
        ORDER BY id DESC
        LIMIT 1
    """, (codigo_cliente, titulo))
    row = cur.fetchone()

    if not row:
        cur.execute("""
            SELECT
                titulo,
                vencimento,
                valor
            FROM cache_titulos
            WHERE codigo_cliente = %s
              AND titulo ILIKE %s
            ORDER BY id DESC
            LIMIT 1
        """, (codigo_cliente, f"%{titulo}%"))
        row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "titulo": row[0] or "",
        "vencimento": formatar_data_input(row[1]),
        "vencimento_display": row[1].strftime("%d/%m/%Y") if row[1] else "",
        "valor": "" if row[2] is None else formatar_moeda(row[2]),
    }


def listar_pedidos_cache(codigo_cliente: str, termo: str = "", limite: int = 20):
    codigo_cliente = (codigo_cliente or "").strip()
    termo = (termo or "").strip()
    if not codigo_cliente:
        return []

    conn = get_conn()
    cur = conn.cursor()
    if termo:
        cur.execute("""
            SELECT DISTINCT numero_pedido
            FROM cache_pedidos
            WHERE codigo_cliente = %s
              AND numero_pedido ILIKE %s
            ORDER BY numero_pedido
            LIMIT %s
        """, (codigo_cliente, f"%{termo}%", limite))
    else:
        cur.execute("""
            SELECT DISTINCT numero_pedido
            FROM cache_pedidos
            WHERE codigo_cliente = %s
            ORDER BY numero_pedido
            LIMIT %s
        """, (codigo_cliente, limite))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[0] for r in rows if r and r[0]]


def listar_titulos_cache(codigo_cliente: str, termo: str = "", limite: int = 20):
    codigo_cliente = (codigo_cliente or "").strip()
    termo = (termo or "").strip()
    if not codigo_cliente:
        return []

    conn = get_conn()
    cur = conn.cursor()
    if termo:
        cur.execute("""
            SELECT DISTINCT titulo
            FROM cache_titulos
            WHERE codigo_cliente = %s
              AND titulo ILIKE %s
            ORDER BY titulo
            LIMIT %s
        """, (codigo_cliente, f"%{termo}%", limite))
    else:
        cur.execute("""
            SELECT DISTINCT titulo
            FROM cache_titulos
            WHERE codigo_cliente = %s
            ORDER BY titulo
            LIMIT %s
        """, (codigo_cliente, limite))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[0] for r in rows if r and r[0]]




def render_base(content: str, title: str = "SAF"):
    nome = session.get("nome", "Usuário")
    codigo = session.get("codigo", "-")
    role = session.get("role", "usuario")
    notificacoes_nao_lidas = contar_notificacoes_internas_nao_lidas(codigo)

    menu_items = [
        ('/dashboard', 'Dashboard'),
        ('/safs', 'SAFs'),
        ('/notificacoes-internas', f'Notificações' + (f' ({notificacoes_nao_lidas})' if notificacoes_nao_lidas else '')),
    ]

    if role in ('atendente', 'supervisor', 'admin'):
        menu_items.append(('/nova-saf', 'Nova SAF'))

    if role == 'admin':
        menu_items.append(('/admin', 'Administração'))

    menu_html = ''.join(
        f'<a href="{href}">{label}</a>' for href, label in menu_items
    )

    flashes = flash_messages_html()
    return render_template_string("""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{{ title }}</title>
        <link rel="icon" type="image/x-icon" href="/logo-kidy-icon">
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; font-family: Arial, Helvetica, sans-serif; }
            body { background: #ffffff; color: #111827; min-height: 100vh; }
            .layout { display: flex; min-height: 100vh; background: #ffffff; }
            .sidebar { width: 270px; background: #ffffff; border-right: 1px solid #e5e7eb; padding: 24px 18px; position: sticky; top: 0; height: 100vh; }
            .brand { margin-bottom: 28px; }
            .brand-logo { width: 160px; max-width: 100%; height: auto; display: block; margin-bottom: 12px; }
            .brand h1 { font-size: 26px; font-weight: 800; margin-bottom: 6px; color: #111827; }
            .brand p { font-size: 13px; color: #6b7280; }
            .user-box { background: #fff7ed; border: 1px solid #fdba74; border-radius: 16px; padding: 14px; margin-bottom: 22px; box-shadow: 0 8px 18px rgba(249,115,22,0.08); }
            .user-box .nome { font-size: 15px; font-weight: 700; color: #111827; margin-bottom: 4px; }
            .user-box .meta { font-size: 12px; color: #6b7280; line-height: 1.5; }
            .nav { display: flex; flex-direction: column; gap: 10px; }
            .nav a { text-decoration: none; color: #111827; padding: 12px 14px; border-radius: 12px; background: #ffffff; border: 1px solid #e5e7eb; transition: 0.2s ease; font-size: 14px; font-weight: 700; }
            .nav a:hover { background: #fff7ed; border-color: #f97316; color: #c2410c; }
            .badge { display:inline-flex; align-items:center; gap:6px; padding:6px 10px; border-radius:999px; font-size:12px; font-weight:700; border:1px solid transparent; }
            .badge-warning { background: #E65100; color:#ffffff; border-color: #E65100; }
            .badge-success { background: #ecfdf5; color:#047857; border-color: #6ee7b7; }
            .badge-danger { background: #B71C1C; color:#ffffff; border-color: #B71C1C; }
            .badge-neutral { background:#f3f4f6; color:#374151; border-color:#d1d5db; }
            .main { flex: 1; padding: 28px; background: #ffffff; }
            .topbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; gap: 16px; flex-wrap: wrap; }
            .topbar-left { display:flex; align-items:center; gap:14px; }
            .topbar-logo { width: 120px; height: auto; display:block; }
            .topbar-title { font-size: 14px; color: #6b7280; }
            .logout-btn { text-decoration: none; background: #f97316; color: white; padding: 10px 16px; border-radius: 12px; font-size: 14px; font-weight: 700; border: none; display: inline-block; box-shadow: 0 8px 18px rgba(249,115,22,0.20); }
            .page-head { margin-bottom: 20px; }
            .page-title { font-size: 32px; font-weight: 800; margin-bottom: 8px; color: #111827; }
            .page-subtitle { color: #6b7280; font-size: 14px; }
            .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 22px; }
            .card, .panel { background: #ffffff; border: 1px solid #e5e7eb; border-radius: 18px; padding: 18px; box-shadow: 0 12px 30px rgba(15,23,42,0.06); }
            .panel { border-radius: 20px; padding: 20px; margin-bottom: 20px; }
            .card-label { color: #6b7280; font-size: 13px; margin-bottom: 10px; }
            .card-value { font-size: 30px; font-weight: 800; color: #111827; }
            .panel h3 { margin-bottom: 14px; font-size: 18px; color: #111827; }
            .panel p, .hint { color: #4b5563; line-height: 1.6; font-size: 14px; }
            .cards-3 { display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:16px; margin-bottom:22px; }
            .small-muted { color:#6b7280; font-size:12px; }
            .pill { display:inline-flex; padding:4px 10px; border-radius:999px; background:#fff7ed; color:#c2410c; font-size:12px; font-weight:700; margin-right:6px; margin-bottom:6px; }
            .grid-2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; margin-bottom: 16px; }
            .field { display: flex; flex-direction: column; }
            .field label { margin-bottom: 8px; color: #374151; font-size: 13px; font-weight: 700; }
            .field input, .field select, .field textarea { width: 100%; border-radius: 14px; border: 1px solid #d1d5db; background: #ffffff; color: #111827; padding: 12px 14px; outline: none; font-size: 14px; }
            .field input::placeholder, .field textarea::placeholder { color: #9ca3af; }
            .field input, .field select { min-height: 46px; }
            .field select { appearance: none; -webkit-appearance: none; -moz-appearance: none; background-image: linear-gradient(45deg, transparent 50%, #6b7280 50%), linear-gradient(135deg, #6b7280 50%, transparent 50%); background-position: calc(100% - 18px) calc(50% - 3px), calc(100% - 12px) calc(50% - 3px); background-size: 6px 6px, 6px 6px; background-repeat: no-repeat; padding-right: 38px; }
            .field select option, .field select optgroup { background: #ffffff; color: #111827; }
            .field textarea { min-height: 110px; resize: vertical; }
            .btn { display: inline-flex; align-items: center; justify-content: center; min-height: 46px; padding: 0 18px; border: none; border-radius: 14px; background: #f97316; color: #ffffff; font-weight: 700; cursor: pointer; text-decoration: none; box-shadow: 0 10px 20px rgba(249,115,22,0.18); }
            .btn:hover { background: #ea580c; }
            .btn-outline { background: #ffffff; color: #c2410c; border: 1px solid #fdba74; box-shadow: none; }
            .btn-danger { background:#ef4444; color:#ffffff; }
            .btn-danger:hover { background:#dc2626; }
            .erro { margin-bottom: 16px; background: #fef2f2; border: 1px solid #fecaca; color: #b91c1c; padding: 12px; border-radius: 12px; font-size: 13px; line-height: 1.45; }
            .table-toolbar { display: flex; justify-content: space-between; align-items: center; gap: 14px; margin-bottom: 14px; flex-wrap: wrap; }
            .saf-table-wrap { overflow-x: auto; }
            .saf-table { width: 100%; border-collapse: separate; border-spacing: 0; }
            .saf-table thead th { text-align: left; font-size: 13px; color: #9a3412; font-weight: 800; padding: 12px 10px; border-bottom: 1px solid #fdba74; background: #fff7ed; }
            .saf-table tbody td { padding: 12px 10px; border-bottom: 1px solid #f3f4f6; vertical-align: top; color: #111827; }
            .saf-table tbody tr:hover td { background: #fff7ed; }
            .linha-urgente td { background:#f77f23; }
            .linha-atrasada td { background:#fc4747; }
            @media (max-width: 920px) { .layout { display:block; } .sidebar { width: 100%; height: auto; position: relative; border-right: 0; border-bottom: 1px solid #e5e7eb; } .grid-2 { grid-template-columns: 1fr; } .main { padding: 20px; } }
        </style>
    </head>
    <body>
        <div class="layout">
            <aside class="sidebar">
                <div class="brand">
                    <img class="brand-logo" src="/logo-kidy" alt="Logo Kidy">
                    <h1>SAF</h1>
                    <p>Solicitação de Ajuste Financeiro</p>
                </div>
                <div class="user-box">
                    <div class="nome">{{ nome }}</div>
                    <div class="meta">Código: {{ codigo }}<br>Perfil: {{ role|capitalize }}</div>
                </div>
                <nav class="nav">{{ menu_html|safe }}</nav>
            </aside>
            <main class="main">
                <div class="topbar">
                    <div class="topbar-left">
                        <img class="topbar-logo" src="/logo-kidy" alt="Logo Kidy">
                        <div class="topbar-title">Painel interno do SAF</div><div class="small-muted">Notificações não lidas: {{ notificacoes_nao_lidas }}</div>
                    </div>
                    <a class="logout-btn" href="/logout">Sair</a>
                </div>
                {{ flashes|safe }}{{ content|safe }}
            </main>
        </div>
    </body>
    </html>
    """, title=title, content=content, nome=nome, codigo=codigo, role=role, menu_html=menu_html, flashes=flashes, notificacoes_nao_lidas=notificacoes_nao_lidas)

# =========================
# ROTAS
# =========================

@app.route("/notificacoes-internas")
@login_required
def notificacoes_internas():
    codigo = usuario_logado_codigo()
    if request.args.get('marcar_todas') == '1':
        total = marcar_todas_notificacoes_internas_lidas(codigo)
        if total:
            flash(f'{total} notificação(ões) marcada(s) como lida(s).', 'success')
        return redirect('/notificacoes-internas')

    notificacoes = listar_notificacoes_internas_usuario(codigo, limite=300)
    linhas = []
    for n in notificacoes:
        badge = '<span class="badge badge-warning">Nova</span>' if not n.get('lida') else '<span class="badge">Lida</span>'
        link_saf = f"<a class='btn btn-outline' href='/saf/{int(n.get('saf_id'))}'>Abrir SAF</a>" if n.get('saf_id') else ''
        acao_ler = '' if n.get('lida') else f"<a class='btn' href='/notificacoes-internas/{int(n.get('id'))}/ler'>Marcar como lida</a>"
        linhas.append(f"""
        <tr>
            <td>{badge}</td>
            <td><strong>{escape(str(n.get('titulo') or '-'))}</strong><div class='small-muted'>{escape(str(n.get('evento') or '-'))}</div></td>
            <td>{escape(str(n.get('mensagem') or '-'))}</td>
            <td>{formatar_data(n.get('criada_em'))}</td>
            <td><div style='display:flex; gap:8px; flex-wrap:wrap;'>{link_saf}{acao_ler}</div></td>
        </tr>
        """)

    content = f"""
    <div class="page-head">
        <div class="page-title">Notificações Internas</div>
        <div class="page-subtitle">Alertas do fluxo SAF dentro do sistema.</div>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <h3>Minhas notificações</h3>
            <div style='display:flex; gap:10px; flex-wrap:wrap;'>
                <span class='hint'>Não lidas: <strong>{contar_notificacoes_internas_nao_lidas(codigo)}</strong></span>
                <a class='btn btn-outline' href='/notificacoes-internas?marcar_todas=1'>Marcar todas como lidas</a>
            </div>
        </div>
        <div class="saf-table-wrap">
            <table class="saf-table">
                <thead>
                    <tr><th>Status</th><th>Título</th><th>Mensagem</th><th>Data</th><th>Ações</th></tr>
                </thead>
                <tbody>{''.join(linhas) if linhas else '<tr><td colspan="5">Nenhuma notificação encontrada.</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    """
    return render_base(content, "Notificações Internas | SAF")


@app.route("/notificacoes-internas/<int:notificacao_id>/ler")
@login_required
def notificacao_interna_ler(notificacao_id):
    marcar_notificacao_interna_lida(notificacao_id, usuario_logado_codigo())
    destino = request.args.get('next') or '/notificacoes-internas'
    return redirect(destino)


@app.route("/admin/painel-notificacoes")
@login_required
@role_required("admin")
def admin_painel_notificacoes():
    codigo = usuario_logado_codigo()
    if request.args.get('marcar_todas') == '1':
        total = marcar_todas_notificacoes_internas_lidas(codigo)
        if total:
            flash(f'{total} notificação(ões) marcada(s) como lida(s).', 'success')
        return redirect('/admin/painel-notificacoes')

    notificacoes = listar_notificacoes_internas_usuario(codigo, limite=500)
    filtro_usuario = texto_limpo(request.args.get('usuario'))
    filtro_evento = texto_limpo(request.args.get('evento'))
    if filtro_usuario:
        notificacoes = [n for n in notificacoes if str(n.get('codigo_usuario') or '').strip().upper() == filtro_usuario.strip().upper()]
    if filtro_evento:
        notificacoes = [n for n in notificacoes if str(n.get('evento') or '').strip().upper() == filtro_evento.strip().upper()]

    usuarios = sorted({str(n.get('codigo_usuario') or '').strip() for n in notificacoes if str(n.get('codigo_usuario') or '').strip()})
    eventos = sorted({str(n.get('evento') or '').strip() for n in notificacoes if str(n.get('evento') or '').strip()})

    linhas = []
    for n in notificacoes:
        badge = '<span class="badge badge-warning">Nova</span>' if not n.get('lida') else '<span class="badge">Lida</span>'
        link_saf = f"<a class='btn btn-outline' href='/saf/{int(n.get('saf_id'))}'>Abrir SAF</a>" if n.get('saf_id') else ''
        acao_ler = '' if n.get('lida') else f"<a class='btn' href='/notificacoes-internas/{int(n.get('id'))}/ler?next=/admin/painel-notificacoes'>Marcar como lida</a>"
        linhas.append(f"""
        <tr>
            <td>{badge}</td>
            <td>{escape(str(n.get('codigo_usuario') or '-'))}</td>
            <td><strong>{escape(str(n.get('titulo') or '-'))}</strong><div class='small-muted'>{escape(str(n.get('evento') or '-'))}</div></td>
            <td>{escape(str(n.get('mensagem') or '-'))}</td>
            <td>{formatar_data(n.get('criada_em'))}</td>
            <td><div style='display:flex; gap:8px; flex-wrap:wrap;'>{link_saf}{acao_ler}</div></td>
        </tr>
        """)

    opcoes_usuarios = ''.join([f"<option value='{escape(u)}' {'selected' if (filtro_usuario or '') == u else ''}>{escape(u)}</option>" for u in usuarios])
    opcoes_eventos = ''.join([f"<option value='{escape(e)}' {'selected' if (filtro_evento or '') == e else ''}>{escape(e)}</option>" for e in eventos])

    content = f"""
    <div class="page-head">
        <div class="page-title">Painel de Notificações do Admin</div>
        <div class="page-subtitle">Visão geral de todas as notificações internas do SAF, sem alterar os demais gerenciamentos do admin.</div>
    </div>

    <div class="panel">
        <form method="get">
            <div class="table-toolbar" style="align-items:end;">
                <div class="field" style="min-width:220px; flex:1;">
                    <label>Usuário</label>
                    <select name="usuario">
                        <option value="">Todos</option>
                        {opcoes_usuarios}
                    </select>
                </div>
                <div class="field" style="min-width:220px; flex:1;">
                    <label>Evento</label>
                    <select name="evento">
                        <option value="">Todos</option>
                        {opcoes_eventos}
                    </select>
                </div>
                <div class="field" style="min-width:120px;">
                    <label>&nbsp;</label>
                    <button class="btn" type="submit">Filtrar</button>
                </div>
                <div class="field" style="min-width:120px;">
                    <label>&nbsp;</label>
                    <a class="btn btn-outline" href="/admin/painel-notificacoes">Limpar</a>
                </div>
            </div>
        </form>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <h3>Todas as notificações</h3>
            <div style='display:flex; gap:10px; flex-wrap:wrap;'>
                <span class='hint'>Não lidas: <strong>{contar_notificacoes_internas_nao_lidas(codigo)}</strong></span>
                <a class='btn btn-outline' href='/admin/painel-notificacoes?marcar_todas=1'>Marcar todas como lidas</a>
                <a class='btn btn-outline' href='/admin'>Voltar</a>
            </div>
        </div>
        <div class="saf-table-wrap">
            <table class="saf-table">
                <thead>
                    <tr><th>Status</th><th>Usuário</th><th>Título</th><th>Mensagem</th><th>Data</th><th>Ações</th></tr>
                </thead>
                <tbody>{''.join(linhas) if linhas else '<tr><td colspan="6">Nenhuma notificação encontrada.</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    """
    return render_base(content, "Painel de Notificações | SAF")


@app.route("/logo-kidy")
def logo_kidy():
    if os.path.exists(ARQ_LOGO_KIDY):
        return send_file(ARQ_LOGO_KIDY)
    return ("Logo não encontrado", 404)

@app.route("/logo-kidy-icon")
def logo_kidy_icon():
    if os.path.exists(ARQ_LOGO_KIDY_ICON):
        return send_file(ARQ_LOGO_KIDY_ICON, mimetype="image/x-icon")
    return ("Ícone não encontrado", 404)


@app.route("/")
def home():
    if "user_id" in session:
        return redirect("/dashboard")
    return redirect("/login")



@app.route("/login", methods=["GET", "POST"])
def login():
    erro = None

    if request.method == "POST":
        identificador = request.form.get("codigo", "").strip()
        senha = request.form.get("senha", "").strip()

        if not identificador or not senha:
            erro = "Preencha usuário e senha."
        else:
            try:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("""
                    SELECT id, codigo_usuario, nome, senha_hash, nivel, regional, ativo
                    FROM usuarios
                    WHERE UPPER(codigo_usuario) = UPPER(%s)
                       OR UPPER(nome) = UPPER(%s)
                    LIMIT 1
                """, (identificador, identificador))
                user = cur.fetchone()
                cur.close()
                conn.close()

                if user:
                    user_id, cod, nome, senha_hash, nivel, regional, ativo = user
                    # ✅ CORRETO
                    if ativo is False:
                        erro = "Usuário inativo."
                    elif senha_hash and verificar_senha(senha, senha_hash):
                        session["user_id"] = user_id
                        session["codigo"] = cod
                        session["nome"] = nome
                        session["role"] = normalizar_nivel_para_role(nivel) if nivel else get_role(cod)
                        session["regional"] = regional
                        return redirect("/dashboard")

                if erro is None:
                    erro = "Usuário ou senha inválidos."

            except Exception as e:
                erro = f"Erro ao conectar no banco: {e}"

    return render_template_string("""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>SAF | Login</title>
        <link rel="icon" type="image/x-icon" href="/logo-kidy-icon">
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; font-family: Arial, Helvetica, sans-serif; }
            body { min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; background: linear-gradient(135deg, #f97316 0%, #fb923c 58%, #fdba74 100%); color: #111827; }
            .box { width: 100%; max-width: 460px; background: rgba(255,255,255,0.98); border: 2px solid rgba(249,115,22,0.18); border-radius: 24px; padding: 34px; box-shadow: 0 24px 60px rgba(154,52,18,0.18); }
            .logo { width: 180px; max-width: 100%; display: block; margin: 0 auto 20px auto; }
            h1 { font-size: 30px; margin-bottom: 8px; color: #111827; text-align: center; }
            p { color: #374151; margin-bottom: 22px; font-size: 14px; text-align: center; }
            label { display: block; margin-bottom: 8px; margin-top: 14px; color: #111827; font-size: 14px; font-weight: 700; }
            input { width: 100%; height: 48px; border-radius: 14px; border: 1px solid #d1d5db; background: #ffffff; color: #111827; padding: 0 14px; outline: none; font-size: 15px; }
            input::placeholder { color: #9ca3af; }
            button { width: 100%; height: 50px; margin-top: 22px; border: none; border-radius: 14px; background: #f97316; color: #ffffff; font-size: 15px; font-weight: 800; cursor: pointer; box-shadow: 0 12px 24px rgba(249,115,22,0.20); }
            button:hover { background: #ea580c; }
            .erro { margin-top: 14px; background: #fef2f2; border: 1px solid #fecaca; color: #dc2626; border-radius: 12px; padding: 12px 14px; font-size: 14px; }
        </style>
    </head>
    <body>
        <div class="box">
            <img class="logo" src="/logo-kidy" alt="Logo Kidy">
            <h1>Entrar no SAF</h1>
            <p>Acesse com seu usuário ou nome e senha.</p>

            <form method="POST">
                <label for="codigo">Usuário</label>
                <input id="codigo" name="codigo" type="text" placeholder="Ex.: ADM001 ou jeane" required>

                <label for="senha">Senha</label>
                <input id="senha" name="senha" type="password" placeholder="Digite sua senha" required>

                <button type="submit">Entrar</button>

                {% if erro %}
                    <div class="erro">{{ erro }}</div>
                {% endif %}
            </form>
        </div>
    </body>
    </html>
    """, erro=erro)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/api/pedido-info")
@login_required
def api_pedido_info():
    codigo_cliente = request.args.get("codigo_cliente", "").strip()
    pedido = request.args.get("pedido", "").strip()

    try:
        dados = buscar_pedido_cache(codigo_cliente, pedido)
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500

    if not dados:
        return jsonify({"ok": False, "encontrado": False})

    return jsonify({"ok": True, "encontrado": True, "dados": dados})


@app.route("/api/titulo-info")
@login_required
def api_titulo_info():
    codigo_cliente = request.args.get("codigo_cliente", "").strip()
    titulo = request.args.get("titulo", "").strip()

    try:
        dados = buscar_titulo_cache(codigo_cliente, titulo)
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500

    if not dados:
        return jsonify({"ok": False, "encontrado": False})

    return jsonify({"ok": True, "encontrado": True, "dados": dados})


@app.route("/api/pedidos-busca")
@login_required
def api_pedidos_busca():
    codigo_cliente = request.args.get("codigo_cliente", "").strip()
    termo = request.args.get("q", "").strip()

    try:
        itens = listar_pedidos_cache(codigo_cliente, termo)
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500

    return jsonify({"ok": True, "itens": itens})


@app.route("/api/cliente-info")
@login_required
def api_cliente_info():
    codigo_cliente = (request.args.get("codigo_cliente") or "").strip()
    if not codigo_cliente:
        return jsonify({"ok": False, "erro": "Informe o código do cliente."}), 400

    try:
        item = buscar_cliente_cache(codigo_cliente)
        if not item:
            return jsonify({"ok": False, "erro": "Cliente não encontrado."}), 404
        return jsonify({"ok": True, "cliente": item})
    except Exception as e:
        return jsonify({"ok": False, "erro": f"Erro ao consultar cliente: {e}"}), 500


@app.route("/api/clientes-busca")
@login_required
def api_clientes_busca():
    termo = (request.args.get("q") or "").strip()
    limite_raw = (request.args.get("limite") or "12").strip()
    try:
        limite = max(1, min(int(limite_raw or "12"), 30))
    except Exception:
        limite = 12

    if len(termo) < 2:
        return jsonify({"ok": True, "itens": []})

    termo_like = f"%{termo}%"
    termo_digits = ''.join(ch for ch in termo if ch.isdigit())
    termo_digits_like = f"%{termo_digits}%" if termo_digits else termo_like

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                codigo_cliente,
                razao_social,
                codigo_representante,
                representante,
                supervisor,
                cidade,
                uf
            FROM cache_clientes
            WHERE
                CAST(COALESCE(codigo_cliente, '') AS TEXT) ILIKE %s
                OR COALESCE(razao_social, '') ILIKE %s
            ORDER BY
                CASE
                    WHEN CAST(COALESCE(codigo_cliente, '') AS TEXT) = %s THEN 0
                    WHEN CAST(COALESCE(codigo_cliente, '') AS TEXT) ILIKE %s THEN 1
                    WHEN COALESCE(razao_social, '') ILIKE %s THEN 2
                    ELSE 3
                END,
                razao_social,
                codigo_cliente
            LIMIT %s
        """, (termo_digits_like, termo_like, termo, termo_like, termo_like, limite))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        itens = []
        for row in rows:
            itens.append({
                "codigo_cliente": row[0] or "",
                "razao_social": row[1] or "",
                "codigo_representante": row[2] or "",
                "representante": row[3] or "",
                "supervisor": row[4] or "",
                "cidade": row[5] or "",
                "uf": row[6] or "",
            })
        return jsonify({"ok": True, "itens": itens})
    except Exception as e:
        return jsonify({"ok": False, "erro": f"Erro ao buscar clientes: {e}"}), 500


@app.route("/api/titulos-cliente")
@login_required
def api_titulos_cliente():
    codigo_cliente = (request.args.get("codigo_cliente") or "").strip()
    q = (request.args.get("q") or "").strip().lower()

    if not codigo_cliente:
        return jsonify({"ok": False, "erro": "Informe o código do cliente."}), 400

    try:
        df = ler_excel_inteligente(ARQ_TITULOS)

        col_codigo_cliente = achar_coluna(df, [
            "codigo_cliente", "cod_cliente", "cliente", "codigo_do_cliente"
        ])
        col_titulo = achar_coluna(df, [
            "titulo", "numero_titulo", "n_titulo", "documento", "numero_documento"
        ])
        col_vencimento = achar_coluna(df, [
            "data_vencimento", "vencimento", "dt_vencimento"
        ])
        col_valor = achar_coluna(df, [
            "valor", "valor_titulo", "vlr_titulo", "valor_documento"
        ])
        col_carteira_desc = achar_coluna(df, [
            "descricao_carteira", "desc_carteira", "carteira_descricao"
        ])
        col_carteira_cod = achar_coluna(df, [
            "codigo_carteira", "cod_carteira", "carteira"
        ])
        col_portador = achar_coluna(df, [
            "banco", "portador", "nome_portador", "descricao_portador"
        ])
        col_df = achar_coluna(df, [
            "df", "data_faturamento"
        ])
        col_dc = achar_coluna(df, [
            "dc", "data_cliente"
        ])
        col_data_saida = achar_coluna(df, [
            "data_saida", "saida"
        ])
        col_data_expedicao = achar_coluna(df, [
            "data_expedicao", "expedicao"
        ])

        if not col_codigo_cliente or not col_titulo:
            return jsonify({
                "ok": False,
                "erro": "A base TITULOS.xlsx não possui as colunas mínimas de código do cliente e título."
            }), 400

        base = df.copy()
        base[col_codigo_cliente] = base[col_codigo_cliente].astype(str).str.strip()
        filtrado = base[base[col_codigo_cliente] == str(codigo_cliente).strip()].copy()

        if q:
            filtrado = filtrado[
                filtrado[col_titulo].astype(str).str.lower().str.contains(q, na=False)
            ].copy()

        def dt_iso(v):
            if v is None or v == "":
                return ""
            dt = data_iso_para_date(v)
            if not dt:
                try:
                    pdt = pd.to_datetime(v, errors="coerce", dayfirst=True)
                    if pd.notna(pdt):
                        dt = pdt.date()
                except Exception:
                    dt = None
            return dt.strftime("%Y-%m-%d") if dt else ""

        registros = []
        for _, row in filtrado.head(300).iterrows():
            valor = numero_brasil_para_float(row.get(col_valor)) if col_valor else None
            despesas = 2.65
            total = (valor or 0) + despesas

            carteira_desc = texto_limpo(row.get(col_carteira_desc)) if col_carteira_desc else ""
            carteira_cod = texto_limpo(row.get(col_carteira_cod)) if col_carteira_cod else ""

            registros.append({
                "titulo": texto_limpo(row.get(col_titulo)),
                "vencimento": dt_iso(row.get(col_vencimento)) if col_vencimento else "",
                "vencimento_original": dt_iso(row.get(col_vencimento)) if col_vencimento else "",
                "novo_vencimento": "",
                "valor": valor if valor is not None else 0,
                "carteira": carteira_desc or carteira_cod or "",
                "portador": texto_limpo(row.get(col_portador)) if col_portador else "",
                "portador_atual": texto_limpo(row.get(col_portador)) if col_portador else "",
                "novo_portador": "1119 (DEVOLUÇÃO)",
                "despesas_financeiras": despesas,
                "total": round(total, 2),
                "df": dt_iso(row.get(col_df)) if col_df else "",
                "dc": dt_iso(row.get(col_dc)) if col_dc else "",
                "data_saida": dt_iso(row.get(col_data_saida)) if col_data_saida else "",
                "data_expedicao": dt_iso(row.get(col_data_expedicao)) if col_data_expedicao else "",
                "data_prevista_entrega": "",
                "percentual_juros": 0,
                "valor_juros": 0,
            })

        return jsonify({
            "ok": True,
            "itens": registros,
            "total_geral": round(sum(float(x.get("total") or 0) for x in registros), 2)
        })

    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/saf/<int:saf_id>/anexos", methods=["POST"])
@login_required
def enviar_anexos_saf(saf_id):
    saf = obter_saf(saf_id)
    if not saf:
        return jsonify({"ok": False, "erro": "SAF não encontrada."}), 404

    if not pode_editar_saf(saf):
        return jsonify({"ok": False, "erro": "Você não tem permissão para anexar arquivos nesta SAF."}), 403

    arquivos = request.files.getlist("arquivos")
    if not arquivos:
        return jsonify({"ok": False, "erro": "Nenhum arquivo foi enviado."}), 400

    garantir_pasta_anexos()
    limite_bytes = MAX_ANEXO_MB * 1024 * 1024

    conn = get_conn()
    cur = conn.cursor()
    enviados = 0

    try:
        for arquivo in arquivos:
            if not arquivo or not (arquivo.filename or "").strip():
                continue

            nome_original = secure_filename(arquivo.filename or "")
            if not nome_original:
                continue

            if not extensao_permitida_anexo(nome_original):
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({"ok": False, "erro": f"Arquivo não permitido: {nome_original}"}), 400

            conteudo = arquivo.read()
            tamanho_bytes = len(conteudo or b"")
            if tamanho_bytes > limite_bytes:
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({"ok": False, "erro": f"O arquivo {nome_original} excede o limite de {MAX_ANEXO_MB} MB."}), 400

            ext = Path(nome_original).suffix.lower()
            nome_salvo = f"{saf_id}_{uuid4().hex}{ext}"
            caminho_arquivo = PASTA_ANEXOS_SAF / nome_salvo
            with open(caminho_arquivo, "wb") as f:
                f.write(conteudo)

            cur.execute("""
                INSERT INTO saf_anexos (
                    saf_id, nome_original, nome_salvo, caminho_arquivo, extensao,
                    tamanho_bytes, mime_type, enviado_por_codigo, enviado_por_nome, enviado_por_perfil
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                saf_id,
                nome_original,
                nome_salvo,
                str(caminho_arquivo),
                ext,
                tamanho_bytes,
                arquivo.mimetype or None,
                usuario_logado_codigo(),
                usuario_logado_nome(),
                usuario_logado_role(),
            ))
            row = cur.fetchone()
            anexo_id = row[0] if row else None
            registrar_log_anexo(cur, saf_id, anexo_id, "UPLOAD", nome_original, "Anexo enviado pela tela da SAF")
            enviados += 1

        if enviados == 0:
            conn.rollback()
            cur.close()
            conn.close()
            return jsonify({"ok": False, "erro": "Nenhum arquivo válido foi enviado."}), 400

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "mensagem": f"{enviados} anexo(s) enviado(s) com sucesso."})
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return jsonify({"ok": False, "erro": str(e)}), 500

@app.route("/saf/anexos/<int:anexo_id>/abrir")
@login_required
def abrir_anexo_saf(anexo_id):
    anexo = obter_anexo_saf(anexo_id)
    if not anexo:
        abort(404)

    saf = obter_saf(anexo.get("saf_id"))
    if not saf or not pode_visualizar_saf(saf):
        abort(403)

    caminho = anexo.get("caminho_arquivo")
    if not caminho or not os.path.exists(caminho):
        return ("Arquivo não encontrado no disco.", 404)

    return send_file(
        caminho,
        mimetype=anexo.get("mime_type") or "application/octet-stream",
        as_attachment=False,
        download_name=anexo.get("nome_original") or os.path.basename(caminho)
    )


@app.route("/saf/anexos/<int:anexo_id>/excluir", methods=["POST"])
@login_required
def excluir_anexo_saf(anexo_id):
    anexo = obter_anexo_saf(anexo_id)
    if not anexo:
        return jsonify({"ok": False, "erro": "Anexo não encontrado."}), 404

    saf = obter_saf(anexo.get("saf_id"))
    if not saf:
        return jsonify({"ok": False, "erro": "SAF não encontrada."}), 404
    if not pode_excluir_anexo_saf(saf, anexo):
        return jsonify({"ok": False, "erro": "Você não tem permissão para excluir este anexo."}), 403

    caminho = anexo.get("caminho_arquivo")
    nome_original = anexo.get("nome_original") or "Arquivo"
    conn = get_conn()
    cur = conn.cursor()
    try:
        # registrar_log_anexo(
        #     cur,
        #     saf_id=saf.get("id"),
        #     anexo_id=anexo.get("id"),
        #     acao="EXCLUIR",
        #     nome_arquivo=nome_original,
        #     observacao="Anexo excluído da SAF."
        # )
        cur.execute("DELETE FROM saf_anexos WHERE id = %s", (anexo_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return jsonify({"ok": False, "erro": str(e)}), 500
    cur.close()
    conn.close()

    try:
        if caminho and os.path.exists(caminho):
            os.remove(caminho)
            pasta = os.path.dirname(caminho)
            if pasta and os.path.isdir(pasta) and not os.listdir(pasta):
                os.rmdir(pasta)
    except Exception:
        pass

    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.is_json:
        return jsonify({"ok": True, "mensagem": "Anexo excluído com sucesso."})

    flash("Anexo excluído com sucesso.", "success")
    return redirect(url_for("visualizar_saf", saf_id=saf.get("id")))


@app.route("/salvar-saf", methods=["POST"])
@login_required
def salvar_saf():
    conn = None
    cur = None
    payload = {}
    try:
        payload = request.get_json(silent=True) or {}
        tipo_saf = (texto_limpo(payload.get("tipo_saf")) or "").strip().upper()
        supervisor = texto_limpo(payload.get("supervisor"))
        codigo_representante = texto_limpo(payload.get("codigo_representante"))
        representante = texto_limpo(payload.get("representante"))
        codigo_cliente = texto_limpo(payload.get("codigo_cliente"))
        razao_social = texto_limpo(payload.get("razao_social"))
        ocorrencia_geral = texto_limpo(payload.get("ocorrencia_geral"))
        prioridade = normalizar_prioridade_saf(payload.get("prioridade"))
        itens = payload.get("itens") or []

        if not tipo_saf:
            return jsonify({"ok": False, "erro": "Tipo de SAF não informado."}), 400
        if tipo_saf not in TIPOS_SAF_VALIDOS:
            return jsonify({"ok": False, "erro": f"Tipo de SAF inválido: {tipo_saf}"}), 400
        if not codigo_cliente and tipo_saf != "INATIVAR_CLIENTE":
            return jsonify({"ok": False, "erro": "Código do cliente não informado."}), 400
        if not isinstance(itens, list) or len(itens) == 0:
            return jsonify({"ok": False, "erro": "Inclua ao menos uma linha na grade antes de salvar."}), 400

        atendente_nome = usuario_logado_nome()
        atendente_codigo = usuario_logado_codigo()
        perfil_criador = usuario_logado_role()

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO saf_solicitacoes (
                tipo_saf, supervisor, codigo_representante, representante,
                atendente_codigo, atendente_nome, codigo_cliente, razao_social,
                ocorrencia_geral, prioridade, status, criado_por_codigo, criado_por_nome,
                perfil_criador, atualizado_em
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            tipo_saf, supervisor, codigo_representante, representante,
            atendente_codigo, atendente_nome, codigo_cliente, razao_social,
            ocorrencia_geral, prioridade,
            'PENDENTE_GERENTE' if perfil_criador == 'supervisor' else 'PENDENTE_SUPERVISOR',
            atendente_codigo, atendente_nome, perfil_criador
        ))
        saf_id = cur.fetchone()[0]

        registros_itens = []
        ordem = 1

        for item in itens:
            pedido = texto_limpo(item.get("pedido"))
            dc = data_iso_para_date(item.get("dc"))
            pares = numero_brasil_para_float(item.get("pares"))
            valor = numero_brasil_para_float(item.get("valor") or 0)
            novo_dc = texto_limpo(item.get("novo_dc"))
            titulo = texto_limpo(item.get("titulo"))
            vencimento = data_iso_para_date(item.get("vencimento"))
            novo_portador = texto_limpo(item.get("novo_portador"))
            despesas_financeiras = numero_brasil_para_float(item.get("despesas_financeiras"))
            vencimento_original = data_iso_para_date(item.get("vencimento_original") or item.get("vencimento"))
            novo_vencimento = data_iso_para_date(item.get("novo_vencimento"))
            data_faturamento = data_iso_para_date(item.get("df"))
            data_saida = data_iso_para_date(item.get("data_saida"))
            data_expedicao = data_iso_para_date(item.get("data_expedicao"))
            data_prevista_entrega = data_iso_para_date(item.get("data_prevista_entrega"))
            percentual_juros = numero_brasil_para_float(item.get("percentual_juros")) or 0
            valor_juros = numero_brasil_para_float(item.get("valor_juros"))
            portador_atual = texto_limpo(item.get("portador_atual") or item.get("portador"))
            carteira_descricao = texto_limpo(item.get("carteira_descricao") or item.get("carteira"))
            total_item = calcular_total_item_saf(tipo_saf, valor, despesas_financeiras, percentual_juros, valor_juros)

            codigo_cliente_item = texto_limpo(item.get("codigo_cliente_item") or item.get("codigo_cliente"))
            razao_social_item = texto_limpo(item.get("razao_social_item") or item.get("razao_social"))
            cnpj_item = texto_limpo(item.get("cnpj_item") or item.get("cnpj"))
            codigo_grupo_cliente_item = texto_limpo(item.get("codigo_grupo_cliente_item") or item.get("codigo_grupo_cliente"))
            grupo_cliente_item = texto_limpo(item.get("grupo_cliente_item") or item.get("grupo_cliente"))

            situacao = texto_limpo(item.get("situacao"))
            acao = texto_limpo(item.get("acao"))
            ocorrencia_item = texto_limpo(item.get("ocorrencia_item"))

            # 🔥 REGRA ESPECÍFICA
            if tipo_saf == "ALTERAR_PORTADOR_DEVOLUCAO":
                if not titulo and not novo_portador:
                    continue
            elif tipo_saf in ("PRORROGAR_SEM_JUROS", "PRORROGAR_COM_JUROS"):
                if not titulo:
                    continue
            else:
                if not any([
                    pedido, dc, pares, valor, novo_dc, titulo, vencimento, novo_portador, despesas_financeiras,
                    codigo_cliente_item, razao_social_item, cnpj_item, codigo_grupo_cliente_item, grupo_cliente_item,
                    situacao, acao, ocorrencia_item
                ]):
                    continue

            registros_itens.append((
                saf_id,
                ordem,
                tipo_saf,
                pedido,
                dc,
                pares,
                valor,
                novo_dc,
                titulo,
                vencimento,
                novo_portador,
                despesas_financeiras,
                total_item,
                vencimento_original,
                novo_vencimento,
                data_faturamento,
                data_saida,
                data_expedicao,
                data_prevista_entrega,
                percentual_juros,
                valor_juros,
                portador_atual,
                carteira_descricao,
                codigo_cliente_item,
                razao_social_item,
                cnpj_item,
                codigo_grupo_cliente_item,
                grupo_cliente_item,
                situacao,
                acao,
                ocorrencia_item
            ))
            ordem += 1

        if not registros_itens:
            cur.close()
            conn.rollback()
            conn.close()
            return jsonify({"ok": False, "erro": "Nenhuma linha válida foi informada para salvar."}), 400

        execute_batch(cur, """
            INSERT INTO saf_itens (
                saf_id, ordem, tipo_saf, pedido, dc, pares, valor, novo_dc,
                titulo, vencimento, novo_portador, despesas_financeiras, total,
                vencimento_original, novo_vencimento, data_faturamento, data_saida, data_expedicao, data_prevista_entrega,
                percentual_juros, valor_juros, portador_atual, carteira_descricao,
                codigo_cliente_item, razao_social_item, cnpj_item, codigo_grupo_cliente_item, grupo_cliente_item,
                situacao, acao, ocorrencia_item
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, registros_itens, page_size=500)

        conn.commit()
        cur.close()
        conn.close()

        try:
            saf_notif = obter_saf(saf_id)
            if saf_notif:
                enviar_notificacoes_whatsapp(
                    saf_notif,
                    'SAF_CRIADA',
                    novo_status=normalizar_status_saf(saf_notif.get('status'))
                )
        except Exception as e:
            print(f"[SAF][Notificacao] Erro ao notificar SAF {saf_id}: {e}")

        return jsonify({"ok": True, "saf_id": saf_id, "mensagem": "SAF salva com sucesso."})

    except Exception as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass

        print(f"[SAF][ERRO_SALVAR] payload={payload}")
        print(f"[SAF][ERRO_SALVAR] erro={repr(e)}")
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/saf/<int:saf_id>/decidir", methods=["POST"])
@login_required
def decidir_saf(saf_id):
    try:
        saf = obter_saf(saf_id)
        if not saf:
            return jsonify({"ok": False, "erro": "SAF não encontrada."}), 404
        if not pode_decidir_saf(saf):
            return jsonify({"ok": False, "erro": "Esta SAF não está na sua etapa de aprovação."}), 403

        payload = request.get_json(silent=True) or {}
        acao = (payload.get("acao") or "").strip().upper()
        observacao = texto_limpo(payload.get("observacao"))
        if acao not in {"APROVAR", "REPROVAR", "OBSERVAR"}:
            return jsonify({"ok": False, "erro": "Ação inválida."}), 400
        if acao in {"OBSERVAR", "REPROVAR"} and not observacao:
            return jsonify({"ok": False, "erro": "Informe a observação."}), 400

        status_atual = normalizar_status_saf(saf.get("status"))
        novo_status = status_atual
        if acao == "APROVAR":
            novo_status = proximo_status_apos_aprovacao(saf)
        elif acao == "REPROVAR":
            novo_status = "REPROVADO"

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE saf_solicitacoes
               SET status = %s,
                   ultima_acao_aprovacao = %s,
                   ultima_observacao_aprovacao = %s,
                   ultimo_aprovador_codigo = %s,
                   ultimo_aprovador_nome = %s,
                   perfil_aprovador = %s,
                   data_aprovacao = CURRENT_TIMESTAMP,
                   atualizado_em = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (
            novo_status,
            acao,
            observacao,
            usuario_logado_codigo(),
            usuario_logado_nome(),
            usuario_logado_role(),
            saf_id,
        ))
        registrar_log_aprovacao(cur, saf_id, acao, observacao)
        conn.commit()
        cur.close()
        conn.close()
        try:
            saf_atualizada = obter_saf(saf_id) or saf
            if acao == 'APROVAR':
                enviar_notificacoes_whatsapp(saf_atualizada, 'SAF_APROVADA', novo_status=novo_status, observacao=observacao)
            elif acao == 'REPROVAR':
                enviar_notificacoes_whatsapp(saf_atualizada, 'SAF_REPROVADA', novo_status=novo_status, observacao=observacao)
            elif acao == 'OBSERVAR':
                enviar_notificacoes_whatsapp(saf_atualizada, 'SAF_OBSERVADA', novo_status=novo_status, observacao=observacao)
        except Exception as e:
            print(f"[SAF][WhatsApp] Erro ao notificar SAF {saf_id}: {e}")
        return jsonify({"ok": True, "mensagem": "Ação registrada com sucesso.", "status": novo_status})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/saf/<int:saf_id>/executar", methods=["POST"])
@login_required
def executar_saf(saf_id):
    try:
        saf = obter_saf(saf_id)
        if not saf:
            return jsonify({"ok": False, "erro": "SAF não encontrada."}), 404
        if not pode_executar_saf(saf):
            return jsonify({"ok": False, "erro": "Você não pode executar esta SAF nesta etapa."}), 403

        payload = request.get_json(silent=True) or {}
        observacao = texto_limpo(payload.get("observacao"))
        status_atual = normalizar_status_saf(saf.get("status"))

        if status_atual == "PENDENTE_FINANCEIRO":
            novo_status = "PENDENTE_AUTENTICACAO_ATENDENTE"
            acao_log = "EXECUCAO_AUTENTICACAO_FINANCEIRO_1"
        elif status_atual == "PENDENTE_EXECUCAO_ATENDENTE":
            novo_status = "PENDENTE_SUPERVISOR_FINANCEIRO"
            acao_log = "EXECUCAO_AUTENTICACAO_ATENDENTE"
        else:
            return jsonify({"ok": False, "erro": "Status inválido para execução."}), 400

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE saf_solicitacoes
               SET status = %s,
                   executado_por_codigo = %s,
                   executado_por_nome = %s,
                   executado_por_perfil = %s,
                   data_execucao = CURRENT_TIMESTAMP,
                   observacao_execucao = %s,
                   atualizado_em = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (
            novo_status,
            usuario_logado_codigo(),
            usuario_logado_nome(),
            usuario_logado_role(),
            observacao,
            saf_id,
        ))
        registrar_log_aprovacao(cur, saf_id, acao_log, observacao)
        conn.commit()
        cur.close()
        conn.close()
        try:
            saf_atualizada = obter_saf(saf_id) or saf
            enviar_notificacoes_whatsapp(saf_atualizada, 'SAF_EXECUTADA', novo_status=novo_status, observacao=observacao)
        except Exception as e:
            print(f"[SAF][Notificacao] Erro ao notificar execução da SAF {saf_id}: {e}")
        return jsonify({"ok": True, "mensagem": "Etapa registrada com sucesso.", "status": novo_status})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/saf/<int:saf_id>/autenticar", methods=["POST"])
@login_required
def autenticar_saf(saf_id):
    try:
        saf = obter_saf(saf_id)
        if not saf:
            return jsonify({"ok": False, "erro": "SAF não encontrada."}), 404
        if not pode_autenticar_saf(saf):
            return jsonify({"ok": False, "erro": "Você não pode autenticar esta SAF nesta etapa."}), 403

        payload = request.get_json(silent=True) or {}
        observacao = texto_limpo(payload.get("observacao"))

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE saf_solicitacoes
               SET status = 'FINALIZADO',
                   autenticado_por_codigo = %s,
                   autenticado_por_nome = %s,
                   autenticado_por_perfil = %s,
                   data_autenticacao = CURRENT_TIMESTAMP,
                   observacao_autenticacao = %s,
                   atualizado_em = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (
            usuario_logado_codigo(),
            usuario_logado_nome(),
            usuario_logado_role(),
            observacao,
            saf_id,
        ))
        registrar_log_aprovacao(cur, saf_id, 'AUTENTICACAO_ATENDENTE', observacao)
        conn.commit()
        cur.close()
        conn.close()
        try:
            saf_atualizada = obter_saf(saf_id) or saf
            enviar_notificacoes_whatsapp(saf_atualizada, 'SAF_FINALIZADA', novo_status='FINALIZADO', observacao=observacao)
        except Exception as e:
            print(f"[SAF][Notificacao] Erro ao notificar finalização da SAF {saf_id}: {e}")
        return jsonify({"ok": True, "mensagem": "Etapa final registrada com sucesso.", "status": 'FINALIZADO'})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/saf/<int:saf_id>/refazer-fluxo", methods=["POST"])
@login_required
def refazer_fluxo_saf(saf_id):
    try:
        saf = obter_saf(saf_id)
        if not saf:
            return jsonify({"ok": False, "erro": "SAF não encontrada."}), 404
        status = normalizar_status_saf(saf.get("status"))
        criador = str(saf.get("criado_por_codigo") or "").strip().upper()
        usuario = str(usuario_logado_codigo() or "").strip().upper()
        if status != "REPROVADO":
            return jsonify({"ok": False, "erro": "Somente SAF reprovada pode refazer o fluxo."}), 400
        if not (usuario_e_admin() or (criador and criador == usuario)):
            return jsonify({"ok": False, "erro": "Somente o criador ou admin pode refazer o fluxo."}), 403

        payload = request.get_json(silent=True) or {}
        observacao = texto_limpo(payload.get("observacao"))

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE saf_solicitacoes
               SET status = %s,
                   ultima_acao_aprovacao = %s,
                   ultima_observacao_aprovacao = %s,
                   ultimo_aprovador_codigo = %s,
                   ultimo_aprovador_nome = %s,
                   perfil_aprovador = %s,
                   data_aprovacao = CURRENT_TIMESTAMP,
                   atualizado_em = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (
            'PENDENTE_SUPERVISOR',
            'REENVIO_FLUXO',
            observacao,
            usuario_logado_codigo(),
            usuario_logado_nome(),
            usuario_logado_role(),
            saf_id,
        ))
        registrar_log_aprovacao(cur, saf_id, 'REENVIO_FLUXO', observacao or 'Fluxo reenviado para Pendente Supervisor.')
        conn.commit()
        cur.close()
        conn.close()
        try:
            saf_atualizada = obter_saf(saf_id) or saf
            enviar_notificacoes_whatsapp(saf_atualizada, 'SAF_ATUALIZADA', novo_status=saf_atualizada.get('status'), observacao=observacao)
        except Exception as e:
            print(f"[SAF][Notificacao] Erro ao notificar SAF {saf_id}: {e}")
        return jsonify({"ok": True, "mensagem": "Fluxo reenviado para Pendente Supervisor.", "status": 'PENDENTE_SUPERVISOR'})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/saf/<int:saf_id>/finalizar", methods=["POST"])
@login_required
def finalizar_saf_reprovada(saf_id):
    try:
        saf = obter_saf(saf_id)
        if not saf:
            return jsonify({"ok": False, "erro": "SAF não encontrada."}), 404
        status = normalizar_status_saf(saf.get("status"))
        criador = str(saf.get("criado_por_codigo") or "").strip().upper()
        usuario = str(usuario_logado_codigo() or "").strip().upper()
        if status != "REPROVADO":
            return jsonify({"ok": False, "erro": "Somente SAF reprovada pode ser finalizada por este botão."}), 400
        if not (usuario_e_admin() or (criador and criador == usuario)):
            return jsonify({"ok": False, "erro": "Somente o criador ou admin pode finalizar a SAF reprovada."}), 403

        payload = request.get_json(silent=True) or {}
        observacao = texto_limpo(payload.get("observacao"))

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE saf_solicitacoes
               SET status = %s,
                   ultima_acao_aprovacao = %s,
                   ultima_observacao_aprovacao = %s,
                   ultimo_aprovador_codigo = %s,
                   ultimo_aprovador_nome = %s,
                   perfil_aprovador = %s,
                   data_aprovacao = CURRENT_TIMESTAMP,
                   atualizado_em = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (
            'FINALIZADO',
            'FINALIZACAO_REPROVADA',
            observacao,
            usuario_logado_codigo(),
            usuario_logado_nome(),
            usuario_logado_role(),
            saf_id,
        ))
        registrar_log_aprovacao(cur, saf_id, 'FINALIZACAO_REPROVADA', observacao or 'SAF reprovada finalizada pelo criador/admin.')
        conn.commit()
        cur.close()
        conn.close()
        try:
            saf_atualizada = obter_saf(saf_id) or saf
            enviar_notificacoes_whatsapp(saf_atualizada, 'SAF_FINALIZADA', novo_status=saf_atualizada.get('status'), observacao=observacao)
        except Exception as e:
            print(f"[SAF][Notificacao] Erro ao notificar SAF {saf_id}: {e}")
        return jsonify({"ok": True, "mensagem": "SAF finalizada com sucesso.", "status": 'FINALIZADO'})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/dashboard")
@login_required
def dashboard():
    metricas = montar_dashboard_metricas()

    status_rows = [
        [escape(label_status_saf(r.get('status'))), str(r.get('total') or 0)]
        for r in metricas['status_rows']
    ]
    tipo_rows = [
        [escape(str(r.get('tipo_saf') or '-')), str(r.get('total') or 0)]
        for r in metricas['tipo_rows']
    ]
    usuario_rows = [
        [
            f"<strong>{escape(str(r.get('nome') or '-'))}</strong><div class='small-muted'>{escape(str(r.get('codigo') or '-'))}</div>",
            str(r.get('qtd_criadas') or 0),
            str(r.get('qtd_aprovacoes') or 0),
            str(r.get('qtd_reprovacoes') or 0),
            str(r.get('qtd_observacoes') or 0),
        ]
        for r in metricas['usuario_rows']
    ]
    notif_rows = [
        [escape(str(r.get('evento') or '-')), escape(str(r.get('status_envio') or '-')), str(r.get('total') or 0)]
        for r in metricas['notif_rows']
    ]

    provider_badge = '<span class="badge badge-success">WhatsApp pronto</span>' if whatsapp_provider_ready() else '<span class="badge badge-warning">WhatsApp pendente de configuração</span>'

    content = f"""
    <div class="page-head">
        <div class="page-title">Dashboard SAF</div>
        <div class="page-subtitle">Indicadores operacionais, aprovações e notificações do fluxo SAF.</div>
    </div>

    <div class="cards">
        <div class="card"><div class="card-label">Minhas SAFs</div><div class="card-value">{metricas['cards']['minhas']}</div></div>
        <div class="card"><div class="card-label">Pendentes</div><div class="card-value">{metricas['cards']['pendentes']}</div></div>
        <div class="card"><div class="card-label">Reprovadas</div><div class="card-value">{metricas['cards']['reprovadas']}</div></div>
        <div class="card"><div class="card-label">Finalizadas</div><div class="card-value">{metricas['cards']['finalizadas']}</div></div>
        <div class="card"><div class="card-label">Total de SAFs</div><div class="card-value">{metricas['cards']['total']}</div></div>
    </div>

    <div class="cards-3">
        <div class="card"><div class="card-label">Tempo médio até 1ª decisão</div><div class="card-value" style="font-size:24px;">{formatar_duracao_horas(metricas['duracoes']['primeira_decisao'])}</div></div>
        <div class="card"><div class="card-label">Tempo médio até finalização</div><div class="card-value" style="font-size:24px;">{formatar_duracao_horas(metricas['duracoes']['finalizacao'])}</div></div>
        <div class="card"><div class="card-label">Tempo médio da última etapa</div><div class="card-value" style="font-size:24px;">{formatar_duracao_horas(metricas['duracoes']['ultima_etapa'])}</div></div>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <h3>Quantidade por status</h3>
            <div class="hint">Contagem consolidada por etapa do fluxo.</div>
        </div>
        <div class="saf-table-wrap">{construir_html_tabela(['Status', 'Quantidade'], status_rows)}</div>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <h3>Quantidade por tipo de SAF</h3>
            <div class="hint">Top tipos mais movimentados.</div>
        </div>
        <div class="saf-table-wrap">{construir_html_tabela(['Tipo SAF', 'Quantidade'], tipo_rows)}</div>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <h3>Quantidade por usuário</h3>
            <div class="hint">Criadas, aprovações, reprovações e observações por usuário.</div>
        </div>
        <div class="saf-table-wrap">{construir_html_tabela(['Usuário', 'Criadas', 'Aprovações', 'Reprovações', 'Observações'], usuario_rows)}</div>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <h3>Notificações WhatsApp</h3>
            <div>{provider_badge}</div>
        </div>
        <p>Quando a SAF é criada, o sistema procura destinatários ativos do perfil <strong>supervisor</strong>. Quando uma SAF é aprovada, o sistema avisa o próximo nível da fila. Para execução, autenticação, reprovação e observação, ele também pode avisar o criador, se houver telefone configurado.</p>
        <div class="saf-table-wrap" style="margin-top:14px;">{construir_html_tabela(['Evento', 'Status do envio', 'Qtde'], notif_rows)}</div>
        {('<p style="margin-top:14px;"><a class="btn" href="/admin/notificacoes">Configurar destinatários WhatsApp</a></p>' if usuario_logado_role() == 'admin' else '')}
    </div>
    """
    return render_base(content, "Dashboard | SAF")


@app.route("/api/inativacao-clientes-busca")
@login_required
def api_inativacao_clientes_busca():
    if usuario_logado_role() not in ("atendente", "supervisor") and not usuario_e_admin():
        return jsonify({"ok": False, "erro": "Acesso negado."}), 403

    codigo_representante = (request.args.get("codigo_representante") or "").strip()
    termo = (request.args.get("q") or "").strip()

    if not codigo_representante:
        return jsonify({"ok": False, "erro": "Informe o código do representante."}), 400

    try:
        itens = buscar_clientes_por_representante(codigo_representante, termo, get_conn)
        return jsonify({"ok": True, "items": itens})
    except Exception as e:
        return jsonify({"ok": False, "erro": f"Erro ao buscar clientes/grupos: {e}"}), 500


@app.route("/nova-saf", methods=["GET", "POST"])
@login_required
def nova_saf():
        erro = None
        tipos_saf_iniciais = [
            ("", "Selecione"),
            ("ALTERAR_PORTADOR_DEVOLUCAO", "ALTERAR PORTADOR PARA DEVOLUÇÃO (MOTIVO) RETORNO DE MERCADORIA"),
            ("ALTERAR_PORTADOR_DIVERSOS", "ALTERAR PORTADOR DIVERSOS (MOTIVO)"),
            ("PRORROGAR_SEM_JUROS", "PRORROGAR SEM JUROS (MOTIVO)"),
            ("PRORROGAR_COM_JUROS", "PRORROGAR COM JUROS (MOTIVO)"),
            ("NEGOCIACAO_TITULOS_REPARCELAMENTO", "NEGOCIAÇÃO DE TÍTULOS - REPARCELAMENTO"),
            ("BAIXAR_CREDITO_CLIENTE", "BAIXAR CRÉDITO DO CLIENTE COM: ADT E/OU DEV"),
            ("CREDITAR_CLIENTE", "CREDITAR O CLIENTE (MOTIVO)"),
            ("CARTA_ANUENCIA_CLIENTE", "CARTA DE ANUÊNCIA (CLIENTE)"),
            ("DESCONTOS_DIVERSOS", "DESCONTOS DIVERSOS"),
            ("INATIVAR_CLIENTE", "INATIVAR CLIENTE"),
        ]

        selected_tipo = request.form.get("tipo_saf", "").strip() if request.method == "POST" else request.args.get("tipo_saf", "").strip()
        codigo_representante_busca = request.form.get("codigo_representante_busca", "").strip() if request.method == "POST" else request.args.get("codigo_representante_busca", "").strip()
        representante_info = None

        if request.method == "POST" and not selected_tipo:
            erro = "Selecione o tipo de SAF para continuar."

        if request.method == "POST" and selected_tipo == "INATIVAR_CLIENTE" and "codigo_representante_busca" in request.form:
            if not codigo_representante_busca:
                erro = "Informe o código do representante para continuar."
            else:
                try:
                    representante_info = buscar_representante_cache(codigo_representante_busca, get_conn)
                    if not representante_info:
                        erro = "Representante não encontrado na base sincronizada de clientes."
                except Exception as e:
                    erro = f"Erro ao consultar representante: {e}"

        feedback = f'<div class="erro">{erro}</div>' if erro else ""
        opcoes_tipo_html = ''.join([
            f'<option value="{valor}" {"selected" if selected_tipo == valor else ""}>{label}</option>'
            for valor, label in tipos_saf_iniciais
        ])

        bloco_principal = ""
        if selected_tipo == "INATIVAR_CLIENTE":
            bloco_principal = f"""
            <div class="panel">
                <h3>Etapa 2 - Base da Inativação</h3>
                <p class="hint" style="margin-bottom:12px;">Esta SAF é diferente das demais: primeiro você informa o <strong>código do representante</strong>. A partir dele o sistema traz representante e supervisor. Depois você seleciona múltiplos clientes/grupos para montar a grade.</p>
                <form method="post">
                    <input type="hidden" name="tipo_saf" value="INATIVAR_CLIENTE">
                    <div class="grid-3">
                        <div class="field">
                            <label>Código do Representante</label>
                            <input type="text" name="codigo_representante_busca" value="{escape(codigo_representante_busca)}" placeholder="Digite o código do representante">
                        </div>
                        <div class="field">
                            <label>Representante</label>
                            <input type="text" value="{escape((representante_info or {}).get('representante') or '')}" readonly>
                        </div>
                        <div class="field">
                            <label>Supervisor</label>
                            <input type="text" value="{escape((representante_info or {}).get('supervisor') or '')}" readonly>
                        </div>
                    </div>
                    <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:8px;">
                        <button class="btn" type="submit">Continuar</button>
                    </div>
                </form>
            </div>
            """
        elif selected_tipo:
            tipo_label = dict(tipos_saf_iniciais).get(selected_tipo, selected_tipo)
            bloco_principal = f"""
            <div class="panel">
                <h3>Tipo selecionado</h3>
                <p><strong>{escape(tipo_label)}</strong></p>
            </div>
            """.replace("__TIPO_SAF_ATUAL__", json.dumps(selected_tipo or ""))

        bloco_formulario = ""
        if representante_info and selected_tipo == "INATIVAR_CLIENTE":
            bloco_formulario = render_nova_saf_inativar_cliente(
                representante_info=representante_info,
                nome_atendente=session.get('nome', ''),
            )
        elif selected_tipo in ("ALTERAR_PORTADOR_DEVOLUCAO", "PRORROGAR_SEM_JUROS", "PRORROGAR_COM_JUROS"):
            dados_form = {
                "codigo_cliente": request.args.get("codigo_cliente", "") or request.form.get("codigo_cliente", "") or "",
                "razao_social": request.args.get("razao_social", "") or request.form.get("razao_social", "") or "",
                "supervisor": request.args.get("supervisor", "") or request.form.get("supervisor", "") or "",
                "codigo_representante": request.args.get("codigo_representante", "") or request.form.get("codigo_representante", "") or "",
                "representante": request.args.get("representante", "") or request.form.get("representante", "") or "",
                "ocorrencia_geral": request.args.get("ocorrencia_geral", "") or request.form.get("ocorrencia_geral", "") or "",
                "prioridade": request.args.get("prioridade", "") or request.form.get("prioridade", "") or "NORMAL",
            }

            if selected_tipo == "PRORROGAR_COM_JUROS":
                bloco_formulario = render_nova_saf_prorrogar_com_juros(
                    dados=dados_form,
                    nome_atendente=session.get('nome', ''),
                )
            else:
                bloco_formulario = render_nova_saf_alterar_portador_devolucao(
                    dados=dados_form,
                    nome_atendente=session.get('nome', ''),
                )

                if selected_tipo == "PRORROGAR_SEM_JUROS":
                    bloco_formulario = bloco_formulario.replace(
                        "ALTERAR PORTADOR PARA DEVOLUÇÃO (MOTIVO) RETORNO DE MERCADORIA",
                        "PRORROGAR SEM JUROS (MOTIVO)"
                    ).replace(
                        "ALTERAR PORTADOR PARA DEVOLUCAO (MOTIVO) RETORNO DE MERCADORIA",
                        "PRORROGAR SEM JUROS (MOTIVO)"
                    )

            bloco_formulario += """
            <style>
            #alterar_portador_multiselecao_ui .titulo-toolbar{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin:0 0 12px 0;}
            #alterar_portador_multiselecao_ui .titulo-card-item{display:grid;grid-template-columns:30px 1fr;gap:12px;padding:12px 14px;border:1px solid #fed7aa;border-radius:14px;background:#fff7ed;margin-bottom:10px;cursor:pointer;}
            #alterar_portador_multiselecao_ui .titulo-card-check{padding-top:2px;}
            #alterar_portador_multiselecao_ui .titulo-card-titulo{font-weight:800;color:#111827;margin-bottom:4px;}
            #alterar_portador_multiselecao_ui .titulo-card-meta{font-size:12px;color:#4b5563;line-height:1.5;}
            #alterar_portador_multiselecao_ui .titulo-actions{display:flex;justify-content:flex-start;align-items:center;margin-top:18px;}
            #alterar_portador_multiselecao_ui .titulo-actions .btn{display:inline-flex !important;align-items:center;justify-content:center;min-height:44px;width:auto !important;padding:0 18px !important;white-space:nowrap;border-radius:12px;}
            </style>
            <script>
            const tipoAtual = __TIPO_SAF_ATUAL__;
            let titulosDisponiveis = [];
            let titulosSelecionados = [];
            let titulosSelecionadosTemp = [];
            let clientesBuscaAtual = [];
            let clienteBuscaTimer = null;

            function numeroSeguro(v) {
                if (v === null || v === undefined || v === '') return 0;
                if (typeof v === 'number') return Number.isFinite(v) ? v : 0;
                const bruto = String(v).trim();
                if (!bruto) return 0;
                const normalizado = bruto.includes(',') ? bruto.replace(/\\./g, '').replace(',', '.') : bruto;
                const n = Number(normalizado);
                return Number.isFinite(n) ? n : 0;
            }

            function moedaBR(v) {
                return numeroSeguro(v).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            }

            function normalizarTexto(txt) {
                return String(txt || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
            }

            function chaveTitulo(item) {
                return [item.titulo || '', item.vencimento || '', item.valor || '', item.portador || '', item.carteira || ''].join('|');
            }

            function atualizarSomaTitulos() {
                const soma = titulosSelecionados.reduce((acc, item) => acc + numeroSeguro(item.total || item.valor || 0), 0);
                document.querySelectorAll('#soma_total_titulos').forEach(el => {
                    el.innerText = moedaBR(soma);
                });
                document.querySelectorAll('#total_geral_titulos').forEach(el => {
                    el.innerText = moedaBR(soma);
                });
            }

            function removerTituloSelecionado(idx) {
                titulosSelecionados.splice(idx, 1);
                renderTitulosSelecionados();
            }

           function renderTitulosSelecionados() {
    const box = document.getElementById('lista_titulos_selecionados');
    if (!box) return;

    const soma = titulosSelecionados.reduce((acc, item) => acc + numeroSeguro(item.total || item.valor || 0), 0);

    if (!titulosSelecionados.length) {
        box.innerHTML = `<div class="hint">Nenhum título selecionado.</div>`;
        return;
    }

    const ehComJuros = tipoAtual === 'PRORROGAR_COM_JUROS';

    const linhas = titulosSelecionados.map((item, idx) => {
        if (ehComJuros) {
            return `
                <tr style="border-top:1px solid #fed7aa;">
                    <td style="padding:10px;vertical-align:top;">${idx + 1}</td>
                    <td style="padding:10px;vertical-align:top;font-weight:700;">${item.titulo || '-'}</td>
                    <td style="padding:10px;vertical-align:top;">${item.vencimento_original || item.vencimento || '-'}</td>
                    <td style="padding:10px;vertical-align:top;"><input type="date" value="${item.novo_vencimento || ''}" onchange="atualizarCampoTitulo(${idx}, 'novo_vencimento', this.value)"></td>
                    <td style="padding:10px;vertical-align:top;text-align:right;">${moedaBR(item.valor)}</td>
                    <td style="padding:10px;vertical-align:top;">${item.carteira || '-'}</td>
                    <td style="padding:10px;vertical-align:top;">${item.portador || '-'}</td>
                    <td style="padding:10px;vertical-align:top;text-align:right;">${moedaBR(item.despesas_financeiras)}</td>
                    <td style="padding:10px;vertical-align:top;">${item.df || '-'}</td>
                    <td style="padding:10px;vertical-align:top;">${item.dc || '-'}</td>
                    <td style="padding:10px;vertical-align:top;">${item.data_saida || '-'}</td>
                    <td style="padding:10px;vertical-align:top;">${item.data_expedicao || '-'}</td>
                    <td style="padding:10px;vertical-align:top;"><input type="date" value="${item.data_prevista_entrega || ''}" onchange="atualizarCampoTitulo(${idx}, 'data_prevista_entrega', this.value)"></td>
                    <td style="padding:10px;vertical-align:top;"><input type="number" step="0.01" min="0" value="${numeroSeguro(item.percentual_juros)}" onchange="atualizarPercentualJuros(${idx}, this.value)" style="width:90px;"></td>
                    <td style="padding:10px;vertical-align:top;text-align:right;">${moedaBR(item.valor_juros || 0)}</td>
                    <td style="padding:10px;vertical-align:top;text-align:right;font-weight:800;">${moedaBR(item.total || item.valor)}</td>
                    <td style="padding:10px;vertical-align:top;text-align:center;">
                        <button class="btn btn-danger" type="button" onclick="removerTituloSelecionado(${idx})">Remover</button>
                    </td>
                </tr>
            `;
        }
        return `
            <tr style="border-top:1px solid #fed7aa;">
                <td style="padding:10px;vertical-align:top;">${idx + 1}</td>
                <td style="padding:10px;vertical-align:top;font-weight:700;">${item.titulo || '-'}</td>
                <td style="padding:10px;vertical-align:top;">${item.vencimento || '-'}</td>
                <td style="padding:10px;vertical-align:top;text-align:right;">${moedaBR(item.valor)}</td>
                <td style="padding:10px;vertical-align:top;">${item.carteira || '-'}</td>
                <td style="padding:10px;vertical-align:top;">${item.portador || '-'}</td>
                <td style="padding:10px;vertical-align:top;">${item.novo_portador || '-'}</td>
                <td style="padding:10px;vertical-align:top;text-align:right;">${moedaBR(item.despesas_financeiras)}</td>
                <td style="padding:10px;vertical-align:top;text-align:right;font-weight:800;">${moedaBR(item.total || item.valor)}</td>
                <td style="padding:10px;vertical-align:top;text-align:center;">
                    <button class="btn btn-danger" type="button" onclick="removerTituloSelecionado(${idx})">Remover</button>
                </td>
            </tr>
        `;
    }).join('');

    const cabecalho = ehComJuros ? `
        <tr style="background:#ffedd5;">
            <th style="padding:12px 10px;text-align:left;">#</th>
            <th style="padding:12px 10px;text-align:left;">Título</th>
            <th style="padding:12px 10px;text-align:left;">Venc. Original</th>
            <th style="padding:12px 10px;text-align:left;">Novo Vencimento</th>
            <th style="padding:12px 10px;text-align:right;">Valor</th>
            <th style="padding:12px 10px;text-align:left;">Carteira</th>
            <th style="padding:12px 10px;text-align:left;">Portador</th>
            <th style="padding:12px 10px;text-align:right;">Desp. Financeira</th>
            <th style="padding:12px 10px;text-align:left;">DF</th>
            <th style="padding:12px 10px;text-align:left;">DC</th>
            <th style="padding:12px 10px;text-align:left;">Data Saída</th>
            <th style="padding:12px 10px;text-align:left;">Data Expedição</th>
            <th style="padding:12px 10px;text-align:left;">Data Prev. Entrega</th>
            <th style="padding:12px 10px;text-align:left;">% Juros</th>
            <th style="padding:12px 10px;text-align:right;">Valor Juros</th>
            <th style="padding:12px 10px;text-align:right;">Total</th>
            <th style="padding:12px 10px;text-align:center;">Ação</th>
        </tr>
    ` : `
        <tr style="background:#ffedd5;">
            <th style="padding:12px 10px;text-align:left;">#</th>
            <th style="padding:12px 10px;text-align:left;">Título</th>
            <th style="padding:12px 10px;text-align:left;">Vencimento</th>
            <th style="padding:12px 10px;text-align:right;">Valor</th>
            <th style="padding:12px 10px;text-align:left;">Carteira</th>
            <th style="padding:12px 10px;text-align:left;">Portador</th>
            <th style="padding:12px 10px;text-align:left;">Novo Portador</th>
            <th style="padding:12px 10px;text-align:right;">Desp. Financeira</th>
            <th style="padding:12px 10px;text-align:right;">Total</th>
            <th style="padding:12px 10px;text-align:center;">Ação</th>
        </tr>
    `;

    box.innerHTML = `
        <div style="width:100%;overflow:auto;border:1px solid #fed7aa;border-radius:18px;background:#fff7ed;">
            <table style="width:100%;min-width:${ehComJuros ? '1800px' : '980px'};border-collapse:collapse;">
                <thead>${cabecalho}</thead>
                <tbody>${linhas}</tbody>
            </table>
        </div>
        <div style="margin-top:14px;display:flex;justify-content:flex-end;">
            <div style="min-width:260px;background:#fff7ed;border:1px solid #fdba74;border-radius:16px;padding:14px 18px;text-align:right;">
                <div style="font-size:12px;color:#9a3412;font-weight:700;letter-spacing:.04em;text-transform:uppercase;">Total geral da SAF</div>
                <div id="total_geral_titulos" style="margin-top:6px;font-size:28px;line-height:1;font-weight:900;color:#ea580c;">${moedaBR(soma)}</div>
            </div>
        </div>
    `;
}

            function garantirDropdownClientes() {
                const input = document.getElementById('codigo_cliente');
                if (!input) return null;
                let box = document.getElementById('cliente_busca_dropdown');
                if (box) return box;
                box = document.createElement('div');
                box.id = 'cliente_busca_dropdown';
                box.style.cssText = 'position:absolute;left:0;right:0;top:calc(100% + 6px);background:#fff;border:1px solid #fed7aa;border-radius:14px;box-shadow:0 16px 32px rgba(0,0,0,.12);z-index:60;max-height:280px;overflow:auto;display:none;';
                const parent = input.parentElement;
                if (parent) {
                    if (getComputedStyle(parent).position === 'static') parent.style.position = 'relative';
                    parent.appendChild(box);
                }
                return box;
            }

            function ocultarDropdownClientes() {
                const box = document.getElementById('cliente_busca_dropdown');
                if (box) box.style.display = 'none';
            }

            function aplicarClienteSelecionado(c) {
                if (!c) return;
                const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = val || ''; };
                set('codigo_cliente', c.codigo_cliente);
                set('razao_social', c.razao_social);
                set('supervisor', c.supervisor);
                set('codigo_representante', c.codigo_representante);
                set('representante', c.representante);
                ocultarDropdownClientes();
            }

            function renderDropdownClientes(lista) {
                const box = garantirDropdownClientes();
                if (!box) return;
                if (!lista || !lista.length) {
                    box.innerHTML = '';
                    box.style.display = 'none';
                    return;
                }
                box.innerHTML = lista.map((c, idx) => `
                    <button type="button" class="cliente-opcao-item" data-idx="${idx}" style="display:block;width:100%;text-align:left;padding:12px 14px;border:none;background:#fff;cursor:pointer;border-bottom:1px solid #ffedd5;">
                        <div style="font-weight:700;color:#111827;">${c.codigo_cliente || '-'} - ${c.razao_social || '-'}</div>
                        <div style="font-size:12px;color:#6b7280;">Rep.: ${c.codigo_representante || '-'} | ${c.representante || '-'} | Sup.: ${c.supervisor || '-'}</div>
                    </button>
                `).join('');
                box.style.display = 'block';
                box.querySelectorAll('.cliente-opcao-item').forEach(btn => {
                    btn.addEventListener('click', () => {
                        const idx = Number(btn.dataset.idx || -1);
                        aplicarClienteSelecionado(clientesBuscaAtual[idx]);
                    });
                });
            }

            async function buscarClientesDigitacao() {
                const input = document.getElementById('codigo_cliente');
                const termo = (input?.value || '').trim();
                if (termo.length < 2) {
                    clientesBuscaAtual = [];
                    ocultarDropdownClientes();
                    return;
                }
                try {
                    const resp = await fetch(`/api/clientes-busca?q=${encodeURIComponent(termo)}`);
                    const data = await resp.json();
                    if (!resp.ok || !data.ok) {
                        clientesBuscaAtual = [];
                        ocultarDropdownClientes();
                        return;
                    }
                    clientesBuscaAtual = Array.isArray(data.itens) ? data.itens : [];
                    renderDropdownClientes(clientesBuscaAtual);
                } catch (e) {
                    clientesBuscaAtual = [];
                    ocultarDropdownClientes();
                }
            }

            async function preencherClientePorCodigo() {
                const codigoCliente = (document.getElementById('codigo_cliente')?.value || '').trim();
                if (!codigoCliente) return;
                try {
                    const resp = await fetch(`/api/cliente-info?codigo_cliente=${encodeURIComponent(codigoCliente)}`);
                    const data = await resp.json();
                    if (!resp.ok || !data.ok || !data.cliente) return;
                    aplicarClienteSelecionado(data.cliente);
                } catch (e) {}
            }

            function garantirEstruturaTitulosUI() {
                const painel = document.querySelector('.panel:last-of-type') || document.querySelector('.panel');
                if (!painel) return;

                const acaoSalvar = tipoAtual === 'PRORROGAR_COM_JUROS'
                    ? 'salvarNovaSafProrrogarComJuros()'
                    : (tipoAtual === 'PRORROGAR_SEM_JUROS'
                        ? 'salvarNovaSafProrrogarSemJuros()'
                        : 'salvarNovaSafAlterarPortador()');

                const salvarBtnExistente = Array.from(document.querySelectorAll('button')).find(btn => /salvar saf/i.test((btn.innerText || '').trim()));
                let host = document.getElementById('alterar_portador_multiselecao_ui');
                if (!host) {
                    host = document.createElement('div');
                    host.id = 'alterar_portador_multiselecao_ui';
                    host.className = 'panel';
                    host.style.marginTop = '16px';
                    host.style.width = '100%';
                    host.innerHTML = `
                        <div class="titulo-toolbar">
                            <div>
                                <div style="font-weight:800;font-size:20px;color:#111827;">Títulos adicionados na SAF</div>
                                <div style="font-size:12px;color:#6b7280;">Selecione um ou vários títulos do cliente e monte a SAF sem repetir itens.</div>
                            </div>
                            <button class="btn" id="btn_abrir_modal_titulos" type="button" onclick="abrirModalTitulos()">Selecionar títulos</button>
                        </div>
                        <div id="lista_titulos_selecionados"></div>
                        <div class="titulo-actions"><button class="btn" type="button" onclick="${acaoSalvar}">Salvar SAF</button></div>
                    `;
                    painel.appendChild(host);
                } else {
                    const btnSalvar = host.querySelector('.titulo-actions .btn');
                    if (btnSalvar) btnSalvar.setAttribute('onclick', acaoSalvar);
                }

                if (salvarBtnExistente && !host.contains(salvarBtnExistente)) {
                    const wrap = salvarBtnExistente.closest('.field, .actions, .grid-2, .grid-3, form, div');
                    if (wrap) wrap.style.display = 'none';
                    else salvarBtnExistente.style.display = 'none';
                }

                if (!document.getElementById('modal_titulos_overlay')) {
                    const modal = document.createElement('div');
                    modal.id = 'modal_titulos_overlay';
                    modal.style.cssText = 'display:none;position:fixed;inset:0;background:rgba(15,23,42,.45);z-index:9999;align-items:center;justify-content:center;padding:18px;';
                    modal.innerHTML = `
                        <div style="width:min(1100px, 100%);max-height:90vh;overflow:auto;background:#fff;border-radius:22px;box-shadow:0 24px 64px rgba(0,0,0,.22);padding:20px;">
                            <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:14px;">
                                <div>
                                    <div style="font-size:20px;font-weight:800;color:#111827;">Selecionar títulos do cliente</div>
                                    <div style="font-size:12px;color:#6b7280;">Marque os títulos que devem entrar nesta SAF.</div>
                                </div>
                                <button class="btn btn-danger" type="button" onclick="fecharModalTitulos()">Fechar</button>
                            </div>
                            <div style="display:grid;grid-template-columns:1.6fr auto auto auto;gap:10px;align-items:center;margin-bottom:12px;">
                                <input id="busca_titulo_modal" type="text" placeholder="Buscar por título, vencimento, carteira ou portador" oninput="filtrarTitulosCliente()" style="width:100%;">
                                <button class="btn" type="button" onclick="carregarTitulosCliente(true)">Atualizar</button>
                                <button class="btn" type="button" onclick="marcarTodosTitulosModal()">Marcar todos</button>
                                <button class="btn btn-secondary" type="button" onclick="limparSelecaoTitulosModal()">Limpar</button>
                            </div>
                            <div id="resumo_titulos_modal" style="margin-bottom:10px;font-weight:700;color:#111827;">Nenhum título selecionado</div>
                            <div id="lista_titulos_modal"></div>
                            <div style="display:flex;justify-content:flex-end;gap:10px;margin-top:16px;">
                                <button class="btn btn-secondary" type="button" onclick="fecharModalTitulos()">Cancelar</button>
                                <button class="btn" type="button" onclick="inserirTitulosSelecionadosModal()">Inserir selecionados</button>
                            </div>
                        </div>
                    `;
                    document.body.appendChild(modal);
                }
            }

            async function carregarTitulosCliente(abrirRender=false) {
                const codigoCliente = (document.getElementById('codigo_cliente')?.value || '').trim();
                if (!codigoCliente) return;
                await preencherClientePorCodigo();
                const q = (document.getElementById('busca_titulo_modal')?.value || '').trim();
                const url = `/api/titulos-cliente?codigo_cliente=${encodeURIComponent(codigoCliente)}${q ? `&q=${encodeURIComponent(q)}` : ''}`;
                const resp = await fetch(url);
                const data = await resp.json();
                if (!resp.ok || !data.ok) { alert(data.erro || 'Erro ao carregar títulos.'); return; }
                titulosDisponiveis = data.itens || [];
                if (abrirRender) renderListaTitulosModal();
            }


            function atualizarCampoTitulo(idx, campo, valor) {
                if (!titulosSelecionados[idx]) return;
                titulosSelecionados[idx][campo] = valor || '';
                renderTitulosSelecionados();
            }

            function atualizarPercentualJuros(idx, valor) {
                if (!titulosSelecionados[idx]) return;
                const pct = numeroSeguro(valor);
                titulosSelecionados[idx].percentual_juros = pct;
                const base = numeroSeguro(titulosSelecionados[idx].valor);
                const juros = base * (pct / 100);
                titulosSelecionados[idx].valor_juros = juros;
                titulosSelecionados[idx].total = base + numeroSeguro(titulosSelecionados[idx].despesas_financeiras) + juros;
                renderTitulosSelecionados();
            }

            function abrirModalTitulos() {
                const codigoCliente = (document.getElementById('codigo_cliente')?.value || '').trim();
                if (!codigoCliente) { alert('Informe o código do cliente antes de selecionar os títulos.'); return; }
                document.getElementById('modal_titulos_overlay').style.display = 'flex';
                titulosSelecionadosTemp = titulosSelecionados.map(item => ({ ...item }));
                const busca = document.getElementById('busca_titulo_modal');
                if (busca) busca.value = '';
                carregarTitulosCliente(true);
            }

            function fecharModalTitulos() {
                const modal = document.getElementById('modal_titulos_overlay');
                if (modal) modal.style.display = 'none';
            }

            function alternarTituloModal(idx, checked) {
                const item = titulosDisponiveis[idx];
                if (!item) return;
                const chave = chaveTitulo(item);
                const pos = titulosSelecionadosTemp.findIndex(x => chaveTitulo(x) === chave);
                if (checked && pos === -1) titulosSelecionadosTemp.push({ ...item });
                else if (!checked && pos >= 0) titulosSelecionadosTemp.splice(pos, 1);
                atualizarResumoModalTitulos();
            }

            function marcarTodosTitulosModal() {
                titulosSelecionadosTemp = titulosDisponiveis.map(item => ({ ...item }));
                renderListaTitulosModal();
            }

            function limparSelecaoTitulosModal() {
                titulosSelecionadosTemp = [];
                renderListaTitulosModal();
            }

            function inserirTitulosSelecionadosModal() {
                titulosSelecionados = titulosSelecionadosTemp.map(item => ({ ...item }));
                renderTitulosSelecionados();
                fecharModalTitulos();
            }

            function atualizarResumoModalTitulos() {
                const alvo = document.getElementById('resumo_titulos_modal');
                if (!alvo) return;
                const qtde = titulosSelecionadosTemp.length;
                const soma = titulosSelecionadosTemp.reduce((acc, item) => acc + numeroSeguro(item.total || item.valor || 0), 0);
                alvo.innerText = qtde ? `${qtde} título(s) selecionado(s) | Soma: ${moedaBR(soma)}` : 'Nenhum título selecionado';
            }

            function filtrarTitulosCliente() { renderListaTitulosModal(); }

            function renderListaTitulosModal() {
                const box = document.getElementById('lista_titulos_modal');
                if (!box) return;
                const termo = normalizarTexto(document.getElementById('busca_titulo_modal')?.value || '');
                const lista = !termo ? titulosDisponiveis : titulosDisponiveis.filter(item => {
                    const texto = normalizarTexto([item.titulo, item.vencimento, item.carteira, item.portador, item.novo_portador].join(' '));
                    return texto.includes(termo);
                });
                if (!lista.length) { box.innerHTML = '<div class="hint">Nenhum título encontrado para este cliente.</div>'; atualizarResumoModalTitulos(); return; }
                box.innerHTML = lista.map(item => {
                    const checked = titulosSelecionadosTemp.some(x => chaveTitulo(x) === chaveTitulo(item)) ? 'checked' : '';
                    const idxOriginal = titulosDisponiveis.findIndex(x => chaveTitulo(x) === chaveTitulo(item));
                    return `
                        <label class="titulo-card-item">
                            <div class="titulo-card-check"><input type="checkbox" ${checked} onchange="alternarTituloModal(${idxOriginal}, this.checked)"></div>
                            <div class="titulo-card-conteudo">
                                <div class="titulo-card-titulo">${item.titulo || '-'}</div>
                                <div class="titulo-card-meta">Vencimento: ${item.vencimento || '-'} | Valor: ${moedaBR(item.valor)} | Carteira: ${item.carteira || '-'}</div>
                                <div class="titulo-card-meta">Portador atual: ${item.portador || '-'} | Novo portador: ${item.novo_portador || '-'} | Desp. financeira: ${moedaBR(item.despesas_financeiras)} | Total: ${moedaBR(item.total || item.valor)}</div>
                            </div>
                        </label>`;
                }).join('');
                atualizarResumoModalTitulos();
            }

            async function salvarNovaSafAlterarPortador() {
                const payload = {
                    tipo_saf: 'ALTERAR_PORTADOR_DEVOLUCAO',
                    codigo_cliente: (document.getElementById('codigo_cliente')?.value || '').trim(),
                    razao_social: (document.getElementById('razao_social')?.value || '').trim(),
                    supervisor: (document.getElementById('supervisor')?.value || '').trim(),
                    codigo_representante: (document.getElementById('codigo_representante')?.value || '').trim(),
                    representante: (document.getElementById('representante')?.value || '').trim(),
                    ocorrencia_geral: (document.getElementById('observacao')?.value || '').trim(),
                    prioridade: (document.getElementById('prioridade')?.value || 'NORMAL').trim(),
                    itens: titulosSelecionados.map(item => ({
                        titulo: item.titulo || '',
                        vencimento: item.vencimento || '',
                        valor: item.valor || 0,
                        novo_portador: item.novo_portador || '1119 (DEVOLUÇÃO)',
                        despesas_financeiras: item.despesas_financeiras || 2.65,
                        ocorrencia_item: '',
                        acao: item.portador || '',
                        situacao: item.carteira || ''
                    }))
                };
                if (!payload.codigo_cliente) { alert('Informe o código do cliente.'); return; }
                if (!payload.itens.length) { alert('Adicione ao menos um título na SAF.'); return; }
                const resp = await fetch('/salvar-saf', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
                const data = await resp.json();
                if (!resp.ok || !data.ok) { alert(data.erro || 'Erro ao salvar SAF.'); return; }
                alert(data.mensagem || 'SAF salva com sucesso.');
                window.location.href = '/saf/' + data.saf_id;
            }


            async function salvarNovaSafProrrogarSemJuros() {
                const payload = {
                    tipo_saf: 'PRORROGAR_SEM_JUROS',
                    codigo_cliente: (document.getElementById('codigo_cliente')?.value || '').trim(),
                    razao_social: (document.getElementById('razao_social')?.value || '').trim(),
                    supervisor: (document.getElementById('supervisor')?.value || '').trim(),
                    codigo_representante: (document.getElementById('codigo_representante')?.value || '').trim(),
                    representante: (document.getElementById('representante')?.value || '').trim(),
                    ocorrencia_geral: (document.getElementById('observacao')?.value || '').trim(),
                    prioridade: (document.getElementById('prioridade')?.value || 'NORMAL').trim(),
                    itens: titulosSelecionados.map(item => ({
                        titulo: item.titulo || '',
                        vencimento: item.vencimento || '',
                        valor: item.valor || 0,
                        novo_portador: item.novo_portador || '1119 (DEVOLUÇÃO)',
                        despesas_financeiras: item.despesas_financeiras || 2.65,
                        ocorrencia_item: '',
                        acao: item.portador || '',
                        situacao: item.carteira || ''
                    }))
                };
                if (!payload.codigo_cliente) { alert('Informe o código do cliente.'); return; }
                if (!payload.itens.length) { alert('Adicione ao menos um título na SAF.'); return; }
                const resp = await fetch('/salvar-saf', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
                const data = await resp.json();
                if (!resp.ok || !data.ok) { alert(data.erro || 'Erro ao salvar SAF.'); return; }
                alert(data.mensagem || 'SAF salva com sucesso.');
                window.location.href = '/saf/' + data.saf_id;
            }


            async function salvarNovaSafProrrogarComJuros() {
                const payload = {
                    tipo_saf: 'PRORROGAR_COM_JUROS',
                    codigo_cliente: (document.getElementById('codigo_cliente')?.value || '').trim(),
                    razao_social: (document.getElementById('razao_social')?.value || '').trim(),
                    supervisor: (document.getElementById('supervisor')?.value || '').trim(),
                    codigo_representante: (document.getElementById('codigo_representante')?.value || '').trim(),
                    representante: (document.getElementById('representante')?.value || '').trim(),
                    ocorrencia_geral: (document.getElementById('observacao')?.value || '').trim(),
                    prioridade: (document.getElementById('prioridade')?.value || 'NORMAL').trim(),
                    itens: titulosSelecionados.map(item => ({
                        titulo: item.titulo || '',
                        vencimento: item.vencimento || '',
                        vencimento_original: item.vencimento_original || item.vencimento || '',
                        novo_vencimento: item.novo_vencimento || '',
                        valor: item.valor || 0,
                        carteira: item.carteira || '',
                        carteira_descricao: item.carteira || '',
                        portador: item.portador || '',
                        portador_atual: item.portador || '',
                        despesas_financeiras: item.despesas_financeiras || 2.65,
                        df: item.df || '',
                        dc: item.dc || '',
                        data_saida: item.data_saida || '',
                        data_expedicao: item.data_expedicao || '',
                        data_prevista_entrega: item.data_prevista_entrega || '',
                        percentual_juros: item.percentual_juros || 0,
                        valor_juros: item.valor_juros || 0,
                        total: item.total || 0,
                        ocorrencia_item: '',
                        acao: item.portador || '',
                        situacao: item.carteira || ''
                    }))
                };
                if (!payload.codigo_cliente) { alert('Informe o código do cliente.'); return; }
                if (!payload.itens.length) { alert('Adicione ao menos um título na SAF.'); return; }
                const semNovoVencimento = payload.itens.some(x => !x.novo_vencimento);
                if (semNovoVencimento) { alert('Preencha o novo vencimento de todos os títulos.'); return; }
                const resp = await fetch('/salvar-saf', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
                const data = await resp.json();
                if (!resp.ok || !data.ok) { alert(data.erro || 'Erro ao salvar SAF.'); return; }
                alert(data.mensagem || 'SAF salva com sucesso.');
                window.location.href = '/saf/' + data.saf_id;
            }

            (function() {
                garantirEstruturaTitulosUI();
                const inputCliente = document.getElementById('codigo_cliente');
                if (inputCliente && !inputCliente.dataset.bindClienteInfo) {
                    inputCliente.dataset.bindClienteInfo = '1';
                    inputCliente.setAttribute('autocomplete', 'off');
                    inputCliente.addEventListener('input', () => {
                        clearTimeout(clienteBuscaTimer);
                        clienteBuscaTimer = setTimeout(() => buscarClientesDigitacao(), 250);
                    });
                    inputCliente.addEventListener('focus', () => buscarClientesDigitacao());
                    inputCliente.addEventListener('blur', () => {
                        setTimeout(() => {
                            ocultarDropdownClientes();
                            preencherClientePorCodigo();
                        }, 180);
                    });
                    inputCliente.addEventListener('keydown', (ev) => {
                        if (ev.key === 'Enter') {
                            ev.preventDefault();
                            if (clientesBuscaAtual.length === 1) aplicarClienteSelecionado(clientesBuscaAtual[0]);
                            else preencherClientePorCodigo();
                        }
                    });
                }
                document.addEventListener('click', (ev) => {
                    const box = document.getElementById('cliente_busca_dropdown');
                    const input = document.getElementById('codigo_cliente');
                    if (!box || !input) return;
                    if (ev.target === input || box.contains(ev.target)) return;
                    ocultarDropdownClientes();
                });
                renderTitulosSelecionados();
            })();
            </script>
            """.replace("__TIPO_SAF_ATUAL__", json.dumps(selected_tipo or ""))



        content = f"""
        <div class="page-head">
            <div class="page-title">Nova SAF</div>
            <div class="page-subtitle">Primeiro selecione o tipo de SAF. Na inativação de clientes, a entrada inicial é pelo código do representante.</div>
        </div>

        {feedback}

        <div class="panel">
            <h3>Etapa 1 - Escolha do tipo</h3>
            <form method="post">
                <div class="grid-2">
                    <div class="field">
                        <label>Tipo de SAF</label>
                        <select name="tipo_saf" required>
                            {opcoes_tipo_html}
                        </select>
                    </div>
                    <div class="field" style="justify-content:end;">
                        <label>&nbsp;</label>
                        <button class="btn" type="submit">Continuar</button>
                    </div>
                </div>
            </form>
        </div>

        {bloco_principal}
        {bloco_formulario}
        """
        return render_base(content, title="Nova SAF")

@app.route("/safs")
@login_required
def listar_safs():
    filtro_status = (request.args.get("status") or "").strip().upper()
    filtro_tipo = (request.args.get("tipo") or "").strip()
    filtro_cliente = (request.args.get("cliente") or "").strip()

    role = usuario_logado_role()
    codigo = usuario_logado_codigo()

    where_parts = []
    params = []

    if role == "atendente" and not usuario_e_admin():
        where_parts.append("criado_por_codigo = %s")
        params.append(codigo)

    if filtro_status:
        where_parts.append("UPPER(COALESCE(status, 'PENDENTE_SUPERVISOR')) = %s")
        params.append(filtro_status)

    if filtro_tipo:
        where_parts.append("UPPER(COALESCE(tipo_saf, '')) LIKE %s")
        params.append(f"%{filtro_tipo.upper()}%")

    if filtro_cliente:
        where_parts.append("(COALESCE(codigo_cliente, '') ILIKE %s OR COALESCE(razao_social, '') ILIKE %s)")
        params.append(f"%{filtro_cliente}%")
        params.append(f"%{filtro_cliente}%")

    where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT id, tipo_saf, data_solicitacao, codigo_cliente, razao_social, prioridade,
            criado_por_nome, criado_por_codigo, status, atualizado_em, supervisor
        FROM saf_solicitacoes
        {where}
        ORDER BY id DESC
        LIMIT 300
    """, tuple(params))
    registros = fetchall_dict(cur)
    cur.close()
    conn.close()

    registros = [reg for reg in registros if pode_visualizar_saf(reg)]

    linhas = ""
    for reg in registros:
        badge = status_badge_html(reg.get("status"))
        atrasada = saf_atrasada(reg)
        badge_prioridade = badge_prioridade_saf(reg.get('prioridade'), atrasada=atrasada)
        status_norm = normalizar_status_saf(reg.get("status"))
        prioridade_norm = normalizar_prioridade_saf(reg.get('prioridade'))

        if status_norm == 'FINALIZADO':
            linha_classe = 'linha-finalizada'
        elif atrasada:
            linha_classe = 'linha-atrasada'
        elif prioridade_norm == 'URGENTE':
            linha_classe = 'linha-urgente'
        else:
            linha_classe = ''

        linhas += f"""
        <tr class="{linha_classe}" style="{estilo_linha_saf(reg)}">
            <td>{reg.get('id')}</td>
            <td>{escape(reg.get('tipo_saf') or '-')}</td>
            <td>{valor_display_data(reg.get('data_solicitacao'))}</td>
            <td>{escape(reg.get('codigo_cliente') or '-')}</td>
            <td>{escape(reg.get('razao_social') or '-')}</td>
            <td>{escape(reg.get('criado_por_nome') or reg.get('criado_por_codigo') or '-')}</td>
            <td>{badge_prioridade}</td>
            <td>{badge}</td>
            <td><a class="btn btn-outline" href="/saf/{reg.get('id')}">Abrir</a></td>
        </tr>
        """

    if not linhas:
        linhas = '<tr><td colspan="9" style="text-align:center; color:#94a3b8;">Nenhuma SAF encontrada.</td></tr>'

    botao_nova_saf = ''
    if session.get("role") in ("atendente", "admin"):
        botao_nova_saf = '<a class="btn" href="/nova-saf">Nova SAF</a>'

    opcoes_status = [
        ('', 'Todos'),
        ('PENDENTE_SUPERVISOR', 'Pendente Supervisor'),
        ('PENDENTE_GERENTE', 'Pendente Gerência'),
        ('PENDENTE_DIRETOR', 'Pendente Diretoria'),
        ('PENDENTE_EXECUCAO_ATENDENTE', 'Pendente Execução Atendente'),
        ('PENDENTE_FINANCEIRO', 'Pendente Financeiro'),
        ('PENDENTE_AUTENTICACAO_ATENDENTE', 'Pendente Autenticação Atendente'),
        ('FINALIZADO', 'Finalizado'),
        ('REPROVADO', 'Reprovado'),
    ]
    select_status = ''.join([
        f'<option value="{valor}" {"selected" if filtro_status == valor else ""}>{label}</option>'
        for valor, label in opcoes_status
    ])

    content = f"""
    <div class="page-head">
        <div class="page-title">Painel de SAFs</div>
        <div class="page-subtitle">Fluxo: Supervisor → Gerência → Diretoria → Supervisor Financeiro/Financeiro → Execução → Autenticação → Finalização.</div>
    </div>

    <div class="panel">
        <form method="get">
            <div class="table-toolbar" style="align-items:end;">
                <div class="field" style="min-width:220px; flex:1;">
                    <label>Status</label>
                    <select name="status">{select_status}</select>
                </div>
                <div class="field" style="min-width:220px; flex:1;">
                    <label>Tipo SAF</label>
                    <input type="text" name="tipo" value="{escape(filtro_tipo)}" placeholder="Ex.: ALTERACAO_DC">
                </div>
                <div class="field" style="min-width:260px; flex:1.2;">
                    <label>Cliente</label>
                    <input type="text" name="cliente" value="{escape(filtro_cliente)}" placeholder="Código ou razão social">
                </div>
                <div class="field"><button class="btn" type="submit">Filtrar</button></div>
                <div class="field"><a class="btn btn-outline" href="/safs">Limpar</a></div>
                <div class="field" style="margin-left:auto;">{botao_nova_saf}</div>
            </div>
        </form>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <div class="hint">Lista das SAFs. Urgente aparece em laranja. Após 2 dias da criação, qualquer SAF pendente aparece em vermelho.</div>
        </div>
        <div class="saf-table-wrap">
            <table class="saf-table">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Tipo</th>
                        <th>Data</th>
                        <th>Cód. Cliente</th>
                        <th>Razão Social</th>
                        <th>Criada por</th>
                        <th>Prioridade</th>
                        <th>Status</th>
                        <th>Ações</th>
                    </tr>
                </thead>
                <tbody>{linhas}</tbody>
            </table>
        </div>
    </div>
    """
    return render_base(content, "Painel de SAFs | SAF")


@app.route("/saf/<int:saf_id>")
@login_required
def visualizar_saf(saf_id):
    saf = obter_saf(saf_id)
    if not saf:
        abort(404)

    if not pode_visualizar_saf(saf):
        abort(403)

    itens = listar_itens_saf(saf_id)
    historico = listar_aprovacoes_saf(saf_id)
    anexos = listar_anexos_saf(saf_id)
    pode_editar = pode_editar_saf(saf)
    pode_decidir = pode_decidir_saf(saf)
    pode_excluir_anexo = pode_excluir_anexo_saf(saf)
    status_html = status_badge_html(saf.get("status"))
    prioridade_html = badge_prioridade_saf(saf.get('prioridade'), atrasada=saf_atrasada(saf))
    tipo_saf = saf.get("tipo_saf") or ""

    itens_json = json.dumps([
        {
            "pedido": item.get("pedido") or "",
            "dc": valor_input_data(item.get("dc")),
            "pares": "" if item.get("pares") is None else str(item.get("pares")),
            "valor": "" if item.get("valor") is None else str(item.get("valor")),
            "novo_dc": item.get("novo_dc") or "",
            "titulo": item.get("titulo") or "",
            "vencimento": valor_input_data(item.get("vencimento")),
            "novo_portador": item.get("novo_portador") or "",
            "despesas_financeiras": "" if item.get("despesas_financeiras") is None else str(item.get("despesas_financeiras")),
            "total": "" if item.get("total") is None else str(item.get("total")),
            "situacao": item.get("situacao") or "",
            "acao": item.get("acao") or "",
            "ocorrencia_item": item.get("ocorrencia_item") or "",
        }
        for item in itens
    ], ensure_ascii=False)
    total_saf = somar_totais_itens_saf(itens)
    exibir_resumo_total = tipo_saf in {"ALTERAR_PORTADOR_DEVOLUCAO", "PRORROGAR_SEM_JUROS"}
    resumo_total_html = ""
    if exibir_resumo_total:
        resumo_total_html = f"""
        <div class="panel">
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:12px;flex-wrap:wrap;">
                <div style="font-size:14px;color:#6b7280;font-weight:700;">Total geral calculado da SAF</div>
                <div id="total_geral_saf_view" style="font-size:26px;font-weight:900;color:#f97316;">R$ {formatar_moeda(total_saf)}</div>
            </div>
        </div>
        """

    historico_html = ""
    for mov in historico:
        historico_html += f"""
        <tr>
            <td>{escape(mov.get('acao') or '-')}</td>
            <td>{escape(mov.get('usuario_nome') or mov.get('usuario_codigo') or '-')}</td>
            <td>{escape(mov.get('usuario_perfil') or '-')}</td>
            <td>{escape(mov.get('observacao') or '-')}</td>
            <td>{formatar_data(mov.get('criado_em'))}</td>
        </tr>
        """
    if not historico_html:
        historico_html = '<tr><td colspan="5" style="text-align:center; color:#94a3b8;">Sem histórico ainda.</td></tr>'

    readonly_attr = "readonly disabled" if not pode_editar else ""
    read_only_js = "false" if pode_editar else "true"
    add_btn_html = '<button class="btn btn-outline" type="button" onclick="adicionarLinha()">Adicionar linha</button>' if pode_editar else ''
    save_btn_html = f'<button class="btn" type="button" onclick="salvarAlteracoes()">Salvar alterações</button>' if pode_editar else ''
    decision_panel = ""
    if pode_decidir:
        titulo_parecer = "Parecer do Supervisor Financeiro" if normalizar_status_saf(saf.get("status")) == "PENDENTE_SUPERVISOR_FINANCEIRO" else "Parecer da gestão"
        decision_panel = f"""
        <div class="panel">
            <h3>{titulo_parecer}</h3>
            <div class="field">
                <label>Observação</label>
                <textarea id="observacao_decisao" placeholder="Digite a observação da aprovação/reprovação"></textarea>
            </div>
            <div class="hint">Na reprovação, a observação é obrigatória.</div>
            <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:12px;">
                <button class="btn" type="button" onclick="registrarDecisao('APROVAR')">Aprovar</button>
                <button class="btn btn-danger" type="button" onclick="registrarDecisao('REPROVAR')">Reprovar</button>
            </div>
        </div>
        """

    execution_panel = ""
    if pode_executar_saf(saf):
        titulo_exec = "Execução e autenticação do Financeiro 2" if normalizar_status_saf(saf.get("status")) == "PENDENTE_FINANCEIRO" else "Execução e autenticação da Atendente"
        execution_panel = f"""
        <div class="panel">
            <h3>{titulo_exec}</h3>
            <div class="field">
                <label>Observação da etapa</label>
                <textarea id="observacao_execucao" placeholder="Descreva a rotina executada no ERP"></textarea>
            </div>
            <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:12px;">
                <button class="btn" type="button" onclick="registrarExecucao()">Confirmar etapa</button>
            </div>
        </div>
        """

    auth_panel = ""
    if pode_autenticar_saf(saf):
        auth_panel = f"""
        <div class="panel">
            <h3>Conferência, autenticação e finalização da Atendente</h3>
            <div class="field">
                <label>Observação da conferência/finalização</label>
                <textarea id="observacao_autenticacao" placeholder="Confirme a finalização da rotina"></textarea>
            </div>
            <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:12px;">
                <button class="btn" type="button" onclick="registrarAutenticacao()">Conferir, autenticar e finalizar</button>
            </div>
        </div>
        """

    reproved_panel = ""
    status_reprovado = normalizar_status_saf(saf.get("status")) == "REPROVADO"
    criador_da_saf = str(saf.get("criado_por_codigo") or "").strip().upper() == str(usuario_logado_codigo() or "").strip().upper()
    if status_reprovado and (usuario_e_admin() or criador_da_saf):
        reproved_panel = f"""
        <div class="panel">
            <h3>SAF reprovada</h3>
            <p class="hint">A SAF voltou para o criador. Você pode corrigir o que for necessário e escolher refazer o fluxo desde o início ou finalizar a solicitação.</p>
            <div class="field">
                <label>Observação do criador</label>
                <textarea id="observacao_reenvio" placeholder="Opcional: descreva o ajuste antes de reenviar ou finalizar"></textarea>
            </div>
            <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:12px;">
                <button class="btn" type="button" onclick="refazerFluxo()">Refazer fluxo</button>
                <button class="btn btn-danger" type="button" onclick="finalizarSafReprovada()">Finalizar SAF</button>
            </div>
        </div>
        """

    anexos_html = ""
    for anexo in anexos:
        acoes_anexo = f'<a class="btn btn-outline" href="/saf/anexos/{anexo.get("id")}/abrir" target="_blank" rel="noopener">Abrir</a>'
        if pode_excluir_anexo:
            acoes_anexo += f'''
            <form method="post" action="/saf/anexos/{anexo.get("id")}/excluir" style="display:inline;" onsubmit="return confirm('Deseja realmente excluir o anexo?');">
                <button class="btn btn-danger" type="submit">Excluir</button>
            </form>'''
        anexos_html += f"""
        <tr>
            <td>{icone_anexo(anexo.get('nome_original'))} {escape(anexo.get('nome_original') or '-')}</td>
            <td>{escape(anexo.get('enviado_por_nome') or anexo.get('enviado_por_codigo') or '-')}</td>
            <td>{escape(anexo.get('enviado_por_perfil') or '-')}</td>
            <td>{escape(tamanho_legivel(anexo.get('tamanho_bytes')))}</td>
            <td>{formatar_data(anexo.get('criado_em'))}</td>
            <td>{acoes_anexo}</td>
        </tr>
        """
    if not anexos_html:
        anexos_html = '<tr><td colspan="6" style="text-align:center; color:#94a3b8;">Nenhum anexo enviado.</td></tr>'

    upload_panel = ""
    if pode_editar:
        upload_panel = """
    <div class="panel">
        <h3>Anexar arquivos</h3>
        <div class="field">
            <label>Selecione imagens, PDF, planilhas ou outros documentos permitidos</label>
            <input type="file" id="anexos_input" multiple>
        </div>
        <div class="hint">Arquivos permitidos: imagens, PDF, Excel, CSV, Word, TXT e compactados. Limite: 20 MB por arquivo.</div>
        <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:12px; align-items:center;">
            <button class="btn btn-outline" type="button" onclick="enviarAnexos()">Enviar anexo(s)</button>
            <span id="anexos_status" class="hint"></span>
        </div>
    </div>
    """

    content = f"""
    <div class="page-head">
        <div class="page-title">SAF #{saf_id}</div>
        <div class="page-subtitle">{escape(tipo_saf)} • {prioridade_html} • {status_html}</div>
    </div>

    <div class="panel">
        <div class="grid-2">
            <div class="field"><label>Tipo SAF</label><input type="text" id="tipo_saf_view" value="{escape(tipo_saf)}" readonly></div>
            <div class="field"><label>Status</label><input type="text" value="{escape(label_status_saf(saf.get('status')))}" readonly></div>
        </div>
        <div class="grid-2">
            <div class="field"><label>Prioridade</label><select id="prioridade_view" {'disabled' if not pode_editar else ''}><option value="NORMAL" {'selected' if normalizar_prioridade_saf(saf.get('prioridade')) == 'NORMAL' else ''}>Normal</option><option value="URGENTE" {'selected' if normalizar_prioridade_saf(saf.get('prioridade')) == 'URGENTE' else ''}>Urgente</option></select></div>
            <div class="field"><label>SLA Diretoria</label><input type="text" value="{'Atrasada (mais de 2 dias)' if saf_atrasada(saf) else 'No prazo'}" readonly></div>
        </div>
        <div class="grid-2">
            <div class="field"><label>Data</label><input type="text" value="{valor_display_data(saf.get('data_solicitacao'))}" readonly></div>
            <div class="field"><label>Criada por</label><input type="text" value="{escape(saf.get('criado_por_nome') or saf.get('criado_por_codigo') or '-') }" readonly></div>
        </div>
        <div class="grid-2">
            <div class="field"><label>Supervisor</label><select id="supervisor_view" {'disabled' if not pode_editar else ''}>{''.join([f'<option value="{escape(s)}" {'selected' if (saf.get('supervisor') or '')==s else ''}>{escape(s)}</option>' for s in SUPERVISORES_FIXOS])}</select></div>
            <div class="field"><label>Código Representante</label><input type="text" id="codigo_representante_view" value="{valor_input_texto(saf.get('codigo_representante'))}" {readonly_attr}></div>
        </div>
        <div class="grid-2">
            <div class="field"><label>Representante</label><input type="text" id="representante_view" value="{valor_input_texto(saf.get('representante'))}" {readonly_attr}></div>
            <div class="field"><label>Código Cliente</label><input type="text" id="codigo_cliente_view" value="{valor_input_texto(saf.get('codigo_cliente'))}" readonly></div>
        </div>
        <div class="field"><label>Razão Social</label><input type="text" id="razao_social_view" value="{valor_input_texto(saf.get('razao_social'))}" readonly></div>
        <div class="field"><label>Ocorrência geral</label><textarea id="ocorrencia_geral_view" {'readonly' if not pode_editar else ''}>{escape(saf.get('ocorrencia_geral') or '')}</textarea></div>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <div class="hint">{('Somente o criador ou administrador pode editar esta grade.' if not pode_editar else 'Edite a grade e salve as alterações.')}</div>
            {add_btn_html}
        </div>
        <div class="saf-table-wrap">
            <table class="saf-table">
                <thead id="saf_detail_thead"></thead>
                <tbody id="saf_detail_tbody"></tbody>
            </table>
        </div>
        {resumo_total_html}
        <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:12px;">{save_btn_html}<a class="btn btn-outline" href="/safs">Voltar</a></div>
    </div>

    {upload_panel}
    <div class="panel">
        <h3>Anexos</h3>
        <div class="saf-table-wrap">
            <table class="saf-table">
                <thead>
                    <tr><th>Arquivo</th><th>Enviado por</th><th>Perfil</th><th>Tamanho</th><th>Data</th><th>Ação</th></tr>
                </thead>
                <tbody>{anexos_html}</tbody>
            </table>
        </div>
    </div>

    {""}

    {decision_panel}
    {execution_panel}
    {auth_panel}
    {reproved_panel}

    <div class="panel">
        <h3>Histórico de observações e decisões</h3>
        <div class="saf-table-wrap">
            <table class="saf-table">
                <thead>
                    <tr><th>Ação</th><th>Usuário</th><th>Perfil</th><th>Observação</th><th>Data</th></tr>
                </thead>
                <tbody>{historico_html}</tbody>
            </table>
        </div>
    </div>

    <script>
        const SAF_ID = {saf_id};
        const SAF_TIPO = {json.dumps(tipo_saf)};
        const CODIGO_CLIENTE = {json.dumps(saf.get('codigo_cliente') or '')};
        const SOMENTE_LEITURA = {read_only_js};
        const initialItens = {itens_json};

        function headCols() {{
            if (SAF_TIPO === 'INATIVAR_CLIENTE') return ['#','Situação','Ação','Ocorrência','Ações'];
            if (SAF_TIPO === 'ALTERACAO_DC') return ['#','Pedido','DC','Pares','Valor','Novo DC','Ocorrência','Ações'];
            if (SAF_TIPO === 'CANCELAMENTO_PEDIDO') return ['#','Pedido','DC','Pares','Valor','Motivo / Observação','Ocorrência','Ações'];
            if (SAF_TIPO === 'ALTERAR_PORTADOR_DEVOLUCAO') return ['#','Título','Vencimento','Valor','Carteira','Portador Atual','Novo Portador','Despesas Financeiras','Total','Ocorrência','Ações'];
            if (SAF_TIPO === 'PRORROGAR_SEM_JUROS') return ['#','Título','Vencimento','Valor','Carteira','Portador Atual','Novo Portador','Despesas Financeiras','Total','Ocorrência','Ações'];
            return ['#','Título','Vencimento','Valor','Novo Portador','Despesas Financeiras','Ocorrência','Ações'];
        }}

        function optionBtnBuscar(tipo) {{
            if (SOMENTE_LEITURA) return '';
            return `<button class="btn btn-outline" type="button" onclick="${{tipo==='pedido' ? 'buscarPedidoLinha(this)' : 'buscarTituloLinha(this)'}}">Buscar</button>`;
        }}

        function linhaHtml(item = {{}}) {{
            const idx = document.querySelectorAll('#saf_detail_tbody tr').length + 1;
            const remBtn = SOMENTE_LEITURA ? '' : `<button class="btn btn-danger" type="button" onclick="removerLinha(this)">Remover</button>`;
            if (SAF_TIPO === 'INATIVAR_CLIENTE') {{
                return `
                    <tr>
                        <td class="line-number">${{idx}}</td>
                        <td><input name="situacao" value="${{item.situacao || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                        <td><input name="acao" value="${{item.acao || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                        <td><textarea name="ocorrencia_item" ${{SOMENTE_LEITURA ? 'readonly' : ''}}>${{item.ocorrencia_item || ''}}</textarea></td>
                        <td>${{remBtn}}</td>
                    </tr>`;
            }}
            if (SAF_TIPO === 'ALTERACAO_DC' || SAF_TIPO === 'CANCELAMENTO_PEDIDO') {{
                const acaoLabel = SAF_TIPO === 'CANCELAMENTO_PEDIDO' ? 'Motivo / Observação' : 'Novo DC';
                const novoCampo = SAF_TIPO === 'CANCELAMENTO_PEDIDO'
                    ? `<textarea name="acao" ${{SOMENTE_LEITURA ? 'readonly' : ''}}>${{item.acao || ''}}</textarea>`
                    : `<input name="novo_dc" value="${{item.novo_dc || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}>`;
                return `
                    <tr>
                        <td class="line-number">${{idx}}</td>
                        <td>
                            <div style="display:flex; gap:8px; align-items:center;">
                                <input list="lista_pedidos_saf_detail" name="pedido" value="${{item.pedido || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}} onblur="autoPedido(this)">
                                ${{SOMENTE_LEITURA ? '' : `<button class="btn btn-outline" type="button" onclick="buscarPedidoLinha(this)">Buscar</button>`}}
                            </div>
                        </td>
                        <td><input name="dc" type="date" value="${{item.dc || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                        <td><input name="pares" value="${{item.pares || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                        <td><input name="valor" value="${{item.valor || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                        <td>${{novoCampo}}</td>
                        <td><textarea name="ocorrencia_item" ${{SOMENTE_LEITURA ? 'readonly' : ''}}>${{item.ocorrencia_item || ''}}</textarea></td>
                        <td>${{remBtn}}</td>
                    </tr>`;
            }}
            if (SAF_TIPO === 'ALTERAR_PORTADOR_DEVOLUCAO' || SAF_TIPO === 'PRORROGAR_SEM_JUROS') {{
                const valorNum = parseFloat(String(item.valor || '0').replace(',', '.')) || 0;
                const despNum = parseFloat(String(item.despesas_financeiras || '0').replace(',', '.')) || 0;
                const totalNum = parseFloat(String(item.total || '0').replace(',', '.')) || (valorNum + despNum);
                return `
                    <tr>
                        <td class="line-number">${{idx}}</td>
                        <td>
                            <div style="display:flex; gap:8px; align-items:center;">
                                <input list="lista_titulos_saf_detail" name="titulo" value="${{item.titulo || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}} onblur="autoTitulo(this)">
                                ${{SOMENTE_LEITURA ? '' : `<button class="btn btn-outline" type="button" onclick="buscarTituloLinha(this)">Buscar</button>`}}
                            </div>
                        </td>
                        <td><input name="vencimento" type="date" value="${{item.vencimento || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                        <td><input name="valor" value="${{item.valor || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}} oninput="recalcularTotaisDetalhe()"></td>
                        <td><input name="situacao" value="${{item.situacao || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                        <td><input name="acao" value="${{item.acao || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                        <td><input name="novo_portador" value="${{item.novo_portador || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                        <td><input name="despesas_financeiras" value="${{item.despesas_financeiras || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}} oninput="recalcularTotaisDetalhe()"></td>
                        <td><input name="total" value="${{totalNum.toFixed(2)}}" readonly style="background:#f8fafc;font-weight:800;"></td>
                        <td><textarea name="ocorrencia_item" ${{SOMENTE_LEITURA ? 'readonly' : ''}}>${{item.ocorrencia_item || ''}}</textarea></td>
                        <td>${{remBtn}}</td>
                    </tr>`;
            }}
            return `
                <tr>
                    <td class="line-number">${{idx}}</td>
                    <td>
                        <div style="display:flex; gap:8px; align-items:center;">
                            <input list="lista_titulos_saf_detail" name="titulo" value="${{item.titulo || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}} onblur="autoTitulo(this)">
                            ${{SOMENTE_LEITURA ? '' : `<button class="btn btn-outline" type="button" onclick="buscarTituloLinha(this)">Buscar</button>`}}
                        </div>
                    </td>
                    <td><input name="vencimento" type="date" value="${{item.vencimento || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                    <td><input name="valor" value="${{item.valor || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                    <td><input name="novo_portador" value="${{item.novo_portador || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                    <td><input name="despesas_financeiras" value="${{item.despesas_financeiras || ''}}" ${{SOMENTE_LEITURA ? 'readonly' : ''}}></td>
                    <td><textarea name="ocorrencia_item" ${{SOMENTE_LEITURA ? 'readonly' : ''}}>${{item.ocorrencia_item || ''}}</textarea></td>
                    <td>${{remBtn}}</td>
                </tr>`;
        }}

        function renderHead() {{
            const cols = headCols().map(col => `<th>${{col}}</th>`).join('');
            document.getElementById('saf_detail_thead').innerHTML = `<tr>${{cols}}</tr>`;
        }}

        function renumerarLinhas() {{
            document.querySelectorAll('#saf_detail_tbody tr').forEach((tr, idx) => {{
                const cel = tr.querySelector('.line-number');
                if (cel) cel.textContent = idx + 1;
            }});
        }}

        function numeroLinhaDetalhe(valor) {{
            const s = String(valor || '').replace(/[.]/g, '').replace(',', '.').trim();
            const n = parseFloat(s);
            return Number.isFinite(n) ? n : 0;
        }}

        function moedaBRDetalhe(valor) {{
            return (Number(valor || 0)).toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
        }}

        function recalcularTotaisDetalhe() {{
            if (!(SAF_TIPO === 'ALTERAR_PORTADOR_DEVOLUCAO' || SAF_TIPO === 'PRORROGAR_SEM_JUROS')) return;
            let soma = 0;
            document.querySelectorAll('#saf_detail_tbody tr').forEach((tr) => {{
                const valorInput = tr.querySelector('input[name="valor"]');
                const despInput = tr.querySelector('input[name="despesas_financeiras"]');
                const totalInput = tr.querySelector('input[name="total"]');
                const total = numeroLinhaDetalhe(valorInput ? valorInput.value : 0) + numeroLinhaDetalhe(despInput ? despInput.value : 0);
                if (totalInput) totalInput.value = total.toFixed(2);
                soma += total;
            }});
            const badge = document.getElementById('total_geral_saf_view');
            if (badge) badge.textContent = 'R$ ' + moedaBRDetalhe(soma);
        }}

        function adicionarLinha(item = {{}}) {{
            if (SOMENTE_LEITURA) return;
            const tbody = document.getElementById('saf_detail_tbody');
            tbody.insertAdjacentHTML('beforeend', linhaHtml(item));
            renumerarLinhas();
        }}

        function removerLinha(btn) {{
            if (SOMENTE_LEITURA) return;
            btn.closest('tr').remove();
            renumerarLinhas();
        }}

        function rowData(tr) {{
            const item = {{}};
            tr.querySelectorAll('input, textarea').forEach(el => item[el.name] = el.value || '');
            return item;
        }}

        async function autoPedido(input) {{
            if (SOMENTE_LEITURA) return;
            const pedido = input.value.trim();
            if (!pedido) return;
            const resp = await fetch(`/api/pedido-info?codigo_cliente=${{encodeURIComponent(CODIGO_CLIENTE)}}&pedido=${{encodeURIComponent(pedido)}}`);
            const data = await resp.json();
            if (!data.ok || !data.encontrado) return;
            const tr = input.closest('tr');
            tr.querySelector('input[name="dc"]').value = data.dados.dc || '';
            tr.querySelector('input[name="pares"]').value = data.dados.pares || '';
            tr.querySelector('input[name="valor"]').value = data.dados.valor || '';
        }}

        async function autoTitulo(input) {{
            if (SOMENTE_LEITURA) return;
            const titulo = input.value.trim();
            if (!titulo) return;
            const resp = await fetch(`/api/titulo-info?codigo_cliente=${{encodeURIComponent(CODIGO_CLIENTE)}}&titulo=${{encodeURIComponent(titulo)}}`);
            const data = await resp.json();
            if (!data.ok || !data.encontrado) return;
            const tr = input.closest('tr');
            tr.querySelector('input[name="vencimento"]').value = data.dados.vencimento || '';
            tr.querySelector('input[name="valor"]').value = data.dados.valor || '';
        }}

        async function buscarPedidoLinha(btn) {{
            const input = btn.parentElement.querySelector('input[name="pedido"]');
            await autoPedido(input);
        }}

        async function buscarTituloLinha(btn) {{
            const input = btn.parentElement.querySelector('input[name="titulo"]');
            await autoTitulo(input);
        }}

        async function popularDatalists() {{
            if (SAF_TIPO === 'ALTERACAO_DC' || SAF_TIPO === 'CANCELAMENTO_PEDIDO') {{
                const resp = await fetch(`/api/pedidos-busca?codigo_cliente=${{encodeURIComponent(CODIGO_CLIENTE)}}`);
                const data = await resp.json();
                if (data.ok) {{
                    let dl = document.getElementById('lista_pedidos_saf_detail');
                    if (!dl) {{
                        dl = document.createElement('datalist');
                        dl.id = 'lista_pedidos_saf_detail';
                        document.body.appendChild(dl);
                    }}
                    dl.innerHTML = (data.itens || []).map(v => `<option value="${{v}}"></option>`).join('');
                }}
            }}
            if (SAF_TIPO === 'TITULOS_DEVOLUCAO') {{
                const resp = await fetch(`/api/titulos-busca?codigo_cliente=${{encodeURIComponent(CODIGO_CLIENTE)}}`);
                const data = await resp.json();
                if (data.ok) {{
                    let dl = document.getElementById('lista_titulos_saf_detail');
                    if (!dl) {{
                        dl = document.createElement('datalist');
                        dl.id = 'lista_titulos_saf_detail';
                        document.body.appendChild(dl);
                    }}
                    dl.innerHTML = (data.itens || []).map(v => `<option value="${{v}}"></option>`).join('');
                }}
            }}
        }}

        async function salvarAlteracoes() {{
            const itens = Array.from(document.querySelectorAll('#saf_detail_tbody tr')).map(rowData);
            const payload = {{
                supervisor: document.getElementById('supervisor_view').value,
                codigo_representante: document.getElementById('codigo_representante_view').value,
                representante: document.getElementById('representante_view').value,
                ocorrencia_geral: document.getElementById('ocorrencia_geral_view').value,
                prioridade: (document.getElementById('prioridade_view')?.value || 'NORMAL'),
                itens
            }};
            const resp = await fetch(`/atualizar-saf/${{SAF_ID}}`, {{
                method:'POST',
                headers: {{'Content-Type':'application/json'}},
                body: JSON.stringify(payload)
            }});
            const data = await resp.json();
            if (!resp.ok || !data.ok) {{
                alert(data.erro || 'Erro ao atualizar SAF.');
                return;
            }}
            alert('SAF atualizada com sucesso.');
            window.location.href='/safs';
        }}

        async function registrarDecisao(acao) {{
            const observacao = document.getElementById('observacao_decisao').value || '';
            const resp = await fetch(`/saf/${{SAF_ID}}/decidir`, {{
                method:'POST',
                headers: {{'Content-Type':'application/json'}},
                body: JSON.stringify({{acao, observacao}})
            }});
            const data = await resp.json();
            if (!resp.ok || !data.ok) {{
                alert(data.erro || 'Erro ao registrar ação.');
                return;
            }}
            alert(data.mensagem || 'Ação registrada.');
            window.location.href='/safs';
        }}

        async function registrarExecucao() {{
            const observacao = document.getElementById('observacao_execucao') ? document.getElementById('observacao_execucao').value : '';
            const resp = await fetch(`/saf/${{SAF_ID}}/executar`, {{
                method:'POST',
                headers: {{'Content-Type':'application/json'}},
                body: JSON.stringify({{observacao}})
            }});
            const data = await resp.json();
            if (!resp.ok || !data.ok) {{
                alert(data.erro || 'Erro ao registrar execução.');
                return;
            }}
            alert(data.mensagem || 'Execução registrada.');
            window.location.href='/safs';
        }}

        async function registrarAutenticacao() {{
            const observacao = document.getElementById('observacao_autenticacao') ? document.getElementById('observacao_autenticacao').value : '';
            const resp = await fetch(`/saf/${{SAF_ID}}/autenticar`, {{
                method:'POST',
                headers: {{'Content-Type':'application/json'}},
                body: JSON.stringify({{observacao}})
            }});
            const data = await resp.json();
            if (!resp.ok || !data.ok) {{
                alert(data.erro || 'Erro ao registrar autenticação.');
                return;
            }}
            alert(data.mensagem || 'Autenticação registrada.');
            window.location.href='/safs';
        }}

        async function refazerFluxo() {{
            const observacao = document.getElementById('observacao_reenvio') ? document.getElementById('observacao_reenvio').value : '';
            const resp = await fetch(`/saf/${{SAF_ID}}/refazer-fluxo`, {{
                method:'POST',
                headers: {{'Content-Type':'application/json'}},
                body: JSON.stringify({{observacao}})
            }});
            const data = await resp.json();
            if (!resp.ok || !data.ok) {{
                alert(data.erro || 'Erro ao refazer fluxo.');
                return;
            }}
            alert(data.mensagem || 'Fluxo reiniciado com sucesso.');
            window.location.href='/safs';
        }}

        async function finalizarSafReprovada() {{
            const observacao = document.getElementById('observacao_reenvio') ? document.getElementById('observacao_reenvio').value : '';
            const resp = await fetch(`/saf/${{SAF_ID}}/finalizar`, {{
                method:'POST',
                headers: {{'Content-Type':'application/json'}},
                body: JSON.stringify({{observacao}})
            }});
            const data = await resp.json();
            if (!resp.ok || !data.ok) {{
                alert(data.erro || 'Erro ao finalizar SAF.');
                return;
            }}
            alert(data.mensagem || 'SAF finalizada com sucesso.');
            window.location.href='/safs';
        }}

        async function excluirAnexo(anexoId, nomeArquivo) {{
            if (!confirm(`Deseja realmente excluir o anexo "${{nomeArquivo}}"?`)) return;
            const resp = await fetch(`/saf/anexos/${{anexoId}}/excluir`, {{
                method: 'POST',
                headers: {{
                    'Content-Type':'application/json',
                    'X-Requested-With':'XMLHttpRequest'
                }}
            }});
            const data = await resp.json();
            if (!resp.ok || !data.ok) {{
                alert(data.erro || 'Erro ao excluir anexo.');
                return;
            }}
            alert(data.mensagem || 'Anexo excluído com sucesso.');
            window.location.reload();
        }}

        async function enviarAnexos() {{
            const input = document.getElementById('anexos_input');
            const status = document.getElementById('anexos_status');
            if (!input || !input.files || !input.files.length) {{
                alert('Selecione pelo menos um arquivo.');
                return;
            }}
            const formData = new FormData();
            for (const arquivo of input.files) {{
                formData.append('arquivos', arquivo);
            }}
            if (status) status.textContent = 'Enviando anexos...';
            const resp = await fetch(`/saf/${{SAF_ID}}/anexos`, {{
                method: 'POST',
                body: formData
            }});
            const data = await resp.json();
            if (!resp.ok || !data.ok) {{
                if (status) status.textContent = '';
                alert(data.erro || 'Erro ao enviar anexos.');
                return;
            }}
            if (status) status.textContent = data.mensagem || 'Anexos enviados com sucesso.';
            window.location.reload();
        }}

        renderHead();
        if (initialItens.length) {{
            const tbody = document.getElementById('saf_detail_tbody');
            initialItens.forEach(item => tbody.insertAdjacentHTML('beforeend', linhaHtml(item)));
            renumerarLinhas();
        }} else if (!SOMENTE_LEITURA) {{
            adicionarLinha();
        }}
        popularDatalists();
    </script>
    """
    return render_base(content, f"SAF #{saf_id} | SAF")


@app.route("/admin")
@login_required
@role_required("admin")
def admin():
    provider_badge = '<span class="badge badge-success">Twilio pronto</span>' if whatsapp_provider_ready() else '<span class="badge badge-warning">Configurar credenciais WhatsApp</span>'
    content = f"""
    <div class="page-head">
        <div class="page-title">Administração</div>
        <div class="page-subtitle">Configurações operacionais do SAF.</div>
    </div>

    <div class="cards-3">
        <div class="card">
            <div class="card-label">Dashboard avançado</div>
            <p>Indicadores por status, usuário, tipo de SAF e tempo médio do fluxo.</p>
            <p style="margin-top:14px;"><a class="btn" href="/dashboard">Abrir dashboard</a></p>
        </div>
        <div class="card">
            <div class="card-label">Credenciais de integração</div>
            <p>Cadastre Twilio, SMTP, porta, usuário, senha, TLS/SSL, remetente e reply-to em um único painel.</p>
            <p style="margin-top:14px;"><a class="btn" href="/admin/credenciais">Configurar credenciais</a></p>
        </div>
        <div class="card">
            <div class="card-label">Destinatários de notificação</div>
            <p>Cadastre quem recebe os alertas por WhatsApp e e-mail conforme o perfil do fluxo.</p>
            <p style="margin-top:14px;"><a class="btn" href="/admin/notificacoes">Configurar destinatários</a></p>
        </div>
    </div>

    <div class="cards-3" style="margin-top:18px;">
        <div class="card">
            <div class="card-label">Usuários</div>
            <p>Cadastre usuários, ajuste e-mail, telefone, perfil, regional, status e redefina senhas.</p>
            <p style="margin-top:14px;"><a class="btn" href="/admin/usuarios">Gerenciar</a></p>
        </div>
        <div class="card">
            <div class="card-label">Painel de notificações</div>
            <p>Visualize todas as notificações internas do sistema sem alterar os demais gerenciamentos do admin.</p>
            <p style="margin-top:14px;"><a class="btn" href="/admin/painel-notificacoes">Abrir painel</a></p>
        </div>
        <div class="card">
            <div class="card-label">Status do WhatsApp</div>
            <p>{provider_badge}</p>
            <p class="small-muted">Provider atual: {escape(WHATSAPP_PROVIDER or '-')}</p>
        </div>
        <div class="card">
            <div class="card-label">Status do e-mail</div>
            <p>{'<span class="badge badge-success">SMTP pronto</span>' if email_provider_ready() else '<span class="badge badge-warning">Configurar SMTP</span>'}</p>
            <p class="small-muted">Acesse o cadastro de credenciais para ajustar servidor, porta, autenticação e remetente.</p>
        </div>
    </div>
    """
    return render_base(content, "Admin | SAF")




@app.route("/admin/credenciais", methods=["GET", "POST"])
@login_required
@role_required("admin")
def admin_credenciais():
    if request.method == 'POST':
        try:
            configs = {
                'WHATSAPP_PROVIDER': (request.form.get('whatsapp_provider') or 'twilio').strip().lower(),
                'TWILIO_ACCOUNT_SID': (request.form.get('twilio_account_sid') or '').strip(),
                'TWILIO_AUTH_TOKEN': (request.form.get('twilio_auth_token') or '').strip(),
                'TWILIO_WHATSAPP_FROM': (request.form.get('twilio_whatsapp_from') or '').strip(),
                'TWILIO_CONTENT_SID': (request.form.get('twilio_content_sid') or '').strip(),
                'WHATSAPP_DEFAULT_COUNTRY_CODE': (request.form.get('whatsapp_default_country_code') or '+55').strip(),
                'WHATSAPP_TIMEOUT': (request.form.get('whatsapp_timeout') or '20').strip(),
                'TWILIO_STATUS_POLL_SECONDS': (request.form.get('twilio_status_poll_seconds') or '3').strip(),
                'TWILIO_STATUS_POLL_ATTEMPTS': (request.form.get('twilio_status_poll_attempts') or '2').strip(),
                'EMAIL_HOST': (request.form.get('email_host') or '').strip(),
                'EMAIL_PORT': (request.form.get('email_port') or '587').strip(),
                'EMAIL_USER': (request.form.get('email_user') or '').strip(),
                'EMAIL_PASS': (request.form.get('email_pass') or '').strip(),
                'EMAIL_FROM': (request.form.get('email_from') or '').strip(),
                'EMAIL_FROM_NAME': (request.form.get('email_from_name') or '').strip(),
                'EMAIL_REPLY_TO': (request.form.get('email_reply_to') or '').strip(),
                'EMAIL_USE_TLS': 'true' if request.form.get('email_use_tls') else 'false',
                'EMAIL_USE_SSL': 'true' if request.form.get('email_use_ssl') else 'false',
                'EMAIL_TIMEOUT': (request.form.get('email_timeout') or '20').strip(),
            }
            salvar_configuracoes_sistema(configs)
            flash('Credenciais salvas com sucesso.', 'success')
        except Exception as e:
            flash(f'Erro ao salvar credenciais: {e}', 'error')
        return redirect('/admin/credenciais')

    cfg = obter_config_runtime_notificacoes()
    resumo = resumo_config_notificacoes()
    content = f"""
    <div class="page-head">
        <div class="page-title">Credenciais de Integração</div>
        <div class="page-subtitle">Salve aqui as credenciais do Twilio e do servidor de e-mail para o SAF.</div>
    </div>

    <div class="panel">
        <h3>Twilio / WhatsApp</h3>
        <form method="post">
            <div class="grid-2">
                <div class="field"><label>Provider</label><input type="text" name="whatsapp_provider" value="{valor_input_texto(cfg.get('WHATSAPP_PROVIDER', 'twilio'))}"></div>
                <div class="field"><label>DDI padrão</label><input type="text" name="whatsapp_default_country_code" value="{valor_input_texto(cfg.get('WHATSAPP_DEFAULT_COUNTRY_CODE', '+55'))}"></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>Account SID</label><input type="text" name="twilio_account_sid" value="{valor_input_texto(cfg.get('TWILIO_ACCOUNT_SID', ''))}"></div>
                <div class="field"><label>Auth Token</label><input type="text" name="twilio_auth_token" value="{valor_input_texto(cfg.get('TWILIO_AUTH_TOKEN', ''))}"></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>Remetente WhatsApp</label><input type="text" name="twilio_whatsapp_from" value="{valor_input_texto(cfg.get('TWILIO_WHATSAPP_FROM', ''))}" placeholder="whatsapp:+14155238886"></div>
                <div class="field"><label>Content SID</label><input type="text" name="twilio_content_sid" value="{valor_input_texto(cfg.get('TWILIO_CONTENT_SID', ''))}"></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>Timeout (s)</label><input type="number" name="whatsapp_timeout" value="{valor_input_texto(cfg.get('WHATSAPP_TIMEOUT', 20))}"></div>
                <div class="field"><label>Status poll (tentativas / segundos)</label><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;"><input type="number" name="twilio_status_poll_attempts" value="{valor_input_texto(cfg.get('TWILIO_STATUS_POLL_ATTEMPTS', 2))}"><input type="number" step="0.1" name="twilio_status_poll_seconds" value="{valor_input_texto(cfg.get('TWILIO_STATUS_POLL_SECONDS', 3))}"></div></div>
            </div>

            <h3 style="margin-top:22px;">Servidor de E-mail</h3>
            <div class="grid-2">
                <div class="field"><label>Servidor SMTP</label><input type="text" name="email_host" value="{valor_input_texto(cfg.get('EMAIL_HOST', ''))}" placeholder="smtp.office365.com"></div>
                <div class="field"><label>Porta</label><input type="number" name="email_port" value="{valor_input_texto(cfg.get('EMAIL_PORT', 587))}"></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>Usuário</label><input type="text" name="email_user" value="{valor_input_texto(cfg.get('EMAIL_USER', ''))}"></div>
                <div class="field"><label>Senha</label><input type="password" name="email_pass" value="{valor_input_texto(cfg.get('EMAIL_PASS', ''))}"></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>Remetente</label><input type="text" name="email_from" value="{valor_input_texto(cfg.get('EMAIL_FROM', ''))}" placeholder="no-reply@empresa.com.br"></div>
                <div class="field"><label>Nome do remetente</label><input type="text" name="email_from_name" value="{valor_input_texto(cfg.get('EMAIL_FROM_NAME', ''))}" placeholder="SAF Kidy"></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>Reply-To</label><input type="text" name="email_reply_to" value="{valor_input_texto(cfg.get('EMAIL_REPLY_TO', ''))}" placeholder="financeiro@empresa.com.br"></div>
                <div class="field"><label>Timeout (s)</label><input type="number" name="email_timeout" value="{valor_input_texto(cfg.get('EMAIL_TIMEOUT', 20))}"></div>
            </div>
            <div class="grid-2">
                <div class="field"><label><input type="checkbox" name="email_use_tls" {'checked' if cfg.get('EMAIL_USE_TLS') else ''}> Usar TLS</label></div>
                <div class="field"><label><input type="checkbox" name="email_use_ssl" {'checked' if cfg.get('EMAIL_USE_SSL') else ''}> Usar SSL</label></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>&nbsp;</label><div class="small-muted">Twilio: {'Pronto' if resumo.get('twilio_configurado') else 'Pendente'} | SMTP: {'Pronto' if resumo.get('email_configurado') else 'Pendente'}</div></div>
                <div class="field"><label>&nbsp;</label><div class="small-muted">Preencha aqui servidor, porta, autenticação, remetente e canal de segurança do SMTP.</div></div>
            </div>
            <p style="margin-top:14px; display:flex; gap:10px; flex-wrap:wrap;"><button class="btn" type="submit">Salvar credenciais</button><a class="btn btn-outline" href="/admin">Voltar</a></p>
        </form>
    </div>
    """
    return render_base(content, 'Credenciais | SAF')


@app.route("/admin/notificacoes", methods=["GET", "POST"])
@login_required
@role_required("admin")
def admin_notificacoes():
    if request.method == 'POST':
        acao = (request.form.get('acao') or '').strip().lower()
        try:
            if acao == 'salvar':
                codigo_usuario = texto_limpo(request.form.get('codigo_usuario'))
                nome_usuario = texto_limpo(request.form.get('nome_usuario'))
                perfil = texto_limpo(request.form.get('perfil'))
                telefone = normalizar_whatsapp(request.form.get('telefone_whatsapp'))
                email_cadastro = obter_email_usuario(codigo_usuario)
                if not codigo_usuario or not perfil:
                    flash('Informe código e perfil.', 'error')
                elif perfil in {'gerente', 'diretor'} and not telefone:
                    flash('Gerente e Diretor exigem WhatsApp no cadastro de notificações.', 'error')
                elif perfil not in {'gerente', 'diretor'} and not email_cadastro:
                    flash('Para esse perfil, o e-mail é buscado no cadastro do usuário. Preencha o e-mail em Administração de Usuários primeiro.', 'error')
                else:
                    salvar_destinatario_whatsapp(
                        codigo_usuario=codigo_usuario.upper(),
                        nome_usuario=nome_usuario or codigo_usuario.upper(),
                        perfil=(perfil or '').strip().lower(),
                        telefone_whatsapp=telefone or '',
                        email=None,
                        ativo=bool(request.form.get('ativo')),
                        recebe_criacao=bool(request.form.get('recebe_criacao')),
                        recebe_aprovacao=bool(request.form.get('recebe_aprovacao')),
                        recebe_reprovacao=bool(request.form.get('recebe_reprovacao')),
                        recebe_observacao=bool(request.form.get('recebe_observacao')),
                    )
                    flash('Destinatário salvo com sucesso.', 'success')
            elif acao == 'excluir':
                dest_id = int(request.form.get('dest_id') or 0)
                if dest_id:
                    excluir_destinatario_whatsapp(dest_id)
                    flash('Destinatário excluído com sucesso.', 'success')
        except Exception as e:
            flash(f'Erro ao salvar destinatário: {e}', 'error')
        return redirect(url_for('admin_notificacoes', edit=request.form.get('codigo_usuario') or ''))

    destinatarios = [enriquecer_destinatario_com_contatos(item) for item in listar_destinatarios_whatsapp()]
    codigo_edicao = (request.args.get('edit') or '').strip().upper()
    destinatario_edicao = None
    if codigo_edicao:
        for item in destinatarios:
            if str(item.get('codigo_usuario') or '').strip().upper() == codigo_edicao:
                destinatario_edicao = item
                break
    linhas = []
    for d in destinatarios:
        badges = []
        if d.get('recebe_criacao'):
            badges.append('<span class="pill">Criação</span>')
        if d.get('recebe_aprovacao'):
            badges.append('<span class="pill">Aprovação</span>')
        if d.get('recebe_reprovacao'):
            badges.append('<span class="pill">Reprovação</span>')
        if d.get('recebe_observacao'):
            badges.append('<span class="pill">Observação</span>')
        badges_html = ''.join(badges) or '-'
        linhas.append(f"""
        <tr>
            <td><strong>{escape(str(d.get('nome_usuario') or '-'))}</strong><div class='small-muted'>{escape(str(d.get('codigo_usuario') or '-'))}</div></td>
            <td>{escape(str(d.get('perfil') or '-').capitalize())}</td>
            <td>{escape(str(d.get('telefone_whatsapp') or '-'))}</td>
            <td>{escape(str(d.get('email') or '-'))}<div class='small-muted'>via cadastro do usuário</div></td>
            <td>{'<span class="badge badge-success">Ativo</span>' if d.get('ativo') else '<span class="badge badge-danger">Inativo</span>'}</td>
            <td>{badges_html}</td>
            <td><div style="display:flex; gap:8px; flex-wrap:wrap;">
                <a class="btn btn-outline" href="/admin/notificacoes?edit={escape(str(d.get('codigo_usuario') or ''))}">Editar</a>
                <form method="post" onsubmit="return confirm('Excluir este destinatário?');">
                    <input type="hidden" name="acao" value="excluir">
                    <input type="hidden" name="dest_id" value="{d.get('id')}">
                    <button class="btn btn-danger" type="submit">Excluir</button>
                </form>
            </div></td>
        </tr>
        """)

    content = f"""
    <div class="page-head">
        <div class="page-title">Notificações SAF</div>
        <div class="page-subtitle">Gerente e Diretor recebem por WhatsApp. Supervisor, Supervisor Financeiro, Financeiro e Atendente recebem por e-mail.</div>
    </div>

    <div class="panel">
        <h3>{"Editar destinatário" if destinatario_edicao else "Novo destinatário"}</h3>
        <form method="post">
            <input type="hidden" name="acao" value="salvar">
            <div class="grid-2">
                <div class="field"><label>Código do usuário</label><input type="text" name="codigo_usuario" value="{valor_input_texto((destinatario_edicao or {}).get('codigo_usuario', ''))}" placeholder="Ex.: COR001" required></div>
                <div class="field"><label>Nome do usuário</label><input type="text" name="nome_usuario" value="{valor_input_texto((destinatario_edicao or {}).get('nome_usuario', ''))}" placeholder="Ex.: Supervisor Teste"></div>
            </div>
            <div class="grid-2">
                <div class="field">
                    <label>Perfil</label>
                    <select name="perfil" required>
                        <option value="supervisor" {'selected' if ((destinatario_edicao or {}).get('perfil') == 'supervisor') else ''}>Supervisor</option>
                        <option value="supervisor_financeiro" {'selected' if ((destinatario_edicao or {}).get('perfil') == 'supervisor_financeiro') else ''}>Supervisor Financeiro</option>
                        <option value="gerente" {'selected' if ((destinatario_edicao or {}).get('perfil') == 'gerente') else ''}>Gerente</option>
                        <option value="diretor" {'selected' if ((destinatario_edicao or {}).get('perfil') == 'diretor') else ''}>Diretor</option>
                        <option value="financeiro" {'selected' if ((destinatario_edicao or {}).get('perfil') == 'financeiro') else ''}>Financeiro</option>
                        <option value="atendente" {'selected' if ((destinatario_edicao or {}).get('perfil') == 'atendente') else ''}>Atendente</option>
                        <option value="admin" {'selected' if ((destinatario_edicao or {}).get('perfil') == 'admin') else ''}>Admin</option>
                    </select>
                </div>
                <div class="field"><label>WhatsApp</label><input type="text" name="telefone_whatsapp" value="{valor_input_texto((destinatario_edicao or {}).get('telefone_whatsapp', ''))}" placeholder="Ex.: +5511999999999"></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>E-mail do usuário</label><input type="text" value="{valor_input_texto((destinatario_edicao or {}).get('email', ''))}" placeholder="Buscado em Administração de Usuários" readonly></div>
                <div class="field"><label><input type="checkbox" name="ativo" {'checked' if (destinatario_edicao is None or (destinatario_edicao or {}).get('ativo')) else ''}> Ativo</label></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>&nbsp;</label><div class="small-muted">Gerente e Diretor usam o WhatsApp informado aqui. Os demais perfis usam o e-mail do cadastro do usuário.</div></div>
                <div class="field"><label>&nbsp;</label><div class="small-muted">Marque os eventos que esse usuário deve receber.</div></div>
            </div>
            <div class="grid-2">
                <div class="field"><label><input type="checkbox" name="recebe_criacao" {'checked' if (destinatario_edicao is None or (destinatario_edicao or {}).get('recebe_criacao')) else ''}> Recebe criação</label></div>
                <div class="field"><label><input type="checkbox" name="recebe_aprovacao" {'checked' if (destinatario_edicao is None or (destinatario_edicao or {}).get('recebe_aprovacao')) else ''}> Recebe aprovação / próximo nível</label></div>
            </div>
            <div class="grid-2">
                <div class="field"><label><input type="checkbox" name="recebe_reprovacao" {'checked' if (destinatario_edicao is None or (destinatario_edicao or {}).get('recebe_reprovacao')) else ''}> Recebe reprovação</label></div>
                <div class="field"><label><input type="checkbox" name="recebe_observacao" {'checked' if ((destinatario_edicao or {}).get('recebe_observacao')) else ''}> Recebe observação</label></div>
            </div>
            <p style="margin-top:14px; display:flex; gap:10px; flex-wrap:wrap;"><button class="btn" type="submit">Salvar destinatário</button><a class="btn btn-outline" href="/admin/notificacoes">Limpar</a></p>
        </form>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <h3>Destinatários cadastrados</h3>
            <div class="hint">WhatsApp: <strong>{escape(WHATSAPP_PROVIDER)}</strong> ({'Pronto' if whatsapp_provider_ready() else 'Pendente'}) | E-mail SMTP: <strong>{escape(EMAIL_HOST)}</strong> ({'Pronto' if email_provider_ready() else 'Pendente'})</div>
        </div>
        <div class="saf-table-wrap">
            <table class="saf-table">
                <thead>
                    <tr>
                        <th>Usuário</th><th>Perfil</th><th>WhatsApp</th><th>E-mail</th><th>Status</th><th>Eventos</th><th>Ações</th>
                    </tr>
                </thead>
                <tbody>{''.join(linhas) if linhas else '<tr><td colspan="7">Nenhum destinatário cadastrado.</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    """
    return render_base(content, "Notificações | SAF")




def garantir_colunas_vinculo_usuarios():
    """
    Garante as colunas de vínculo na tabela usuarios mesmo em bases já existentes.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS supervisor_codigo VARCHAR(50)")
        cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS gerente_codigo VARCHAR(50)")
        conn.commit()
    finally:
        cur.close()
        conn.close()


def colunas_vinculo_usuarios_existentes():
    """
    Retorna as colunas reais existentes para vínculo. Aceita nomes alternativos caso o banco
    tenha sido ajustado manualmente com nomenclatura diferente.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = 'usuarios'
        """)
        cols = {str(r[0]).strip().lower() for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()

    candidatos_supervisor = [
        'supervisor_codigo',
        'codigo_supervisor',
        'supervisor_cod',
        'cod_supervisor',
    ]
    candidatos_gerente = [
        'gerente_codigo',
        'codigo_gerente',
        'gerente_cod',
        'cod_gerente',
    ]

    col_supervisor = next((c for c in candidatos_supervisor if c in cols), None)
    col_gerente = next((c for c in candidatos_gerente if c in cols), None)

    return col_supervisor, col_gerente


def listar_usuarios_admin():
    garantir_colunas_vinculo_usuarios()
    col_supervisor, col_gerente = colunas_vinculo_usuarios_existentes()
    expr_supervisor = f"u.{col_supervisor}" if col_supervisor else "NULL"
    expr_gerente = f"u.{col_gerente}" if col_gerente else "NULL"

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(f"""
            SELECT
                u.id,
                u.codigo_usuario,
                u.nome,
                u.nivel,
                u.regional,
                u.ativo,
                u.email,
                u.telefone,
                {expr_supervisor} AS supervisor_codigo,
                {expr_gerente} AS gerente_codigo,
                sup.nome AS supervisor_nome,
                ger.nome AS gerente_nome,
                u.criado_em,
                u.atualizado_em
            FROM usuarios u
            LEFT JOIN usuarios sup ON UPPER(COALESCE(sup.codigo_usuario, '')) = UPPER(COALESCE({expr_supervisor}::text, ''))
            LEFT JOIN usuarios ger ON UPPER(COALESCE(ger.codigo_usuario, '')) = UPPER(COALESCE({expr_gerente}::text, ''))
            ORDER BY u.nome, u.codigo_usuario
        """)
        rows = cur.fetchall()
        return rows or []
    finally:
        cur.close()
        conn.close()


def obter_usuario_admin(codigo_usuario: str):
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario:
        return None

    garantir_colunas_vinculo_usuarios()
    col_supervisor, col_gerente = colunas_vinculo_usuarios_existentes()
    expr_supervisor = f"u.{col_supervisor}" if col_supervisor else "NULL"
    expr_gerente = f"u.{col_gerente}" if col_gerente else "NULL"

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(f"""
            SELECT
                u.id,
                u.codigo_usuario,
                u.nome,
                u.nivel,
                u.regional,
                u.ativo,
                u.email,
                u.telefone,
                {expr_supervisor} AS supervisor_codigo,
                {expr_gerente} AS gerente_codigo,
                sup.nome AS supervisor_nome,
                ger.nome AS gerente_nome,
                u.criado_em,
                u.atualizado_em
            FROM usuarios u
            LEFT JOIN usuarios sup ON UPPER(COALESCE(sup.codigo_usuario, '')) = UPPER(COALESCE({expr_supervisor}::text, ''))
            LEFT JOIN usuarios ger ON UPPER(COALESCE(ger.codigo_usuario, '')) = UPPER(COALESCE({expr_gerente}::text, ''))
            WHERE UPPER(COALESCE(u.codigo_usuario, '')) = UPPER(%s)
            LIMIT 1
        """, (codigo_usuario,))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def salvar_usuario_admin(codigo_usuario: Optional[str]):
    codigo_usuario = texto_limpo(codigo_usuario)
    if not codigo_usuario:
        return None
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT
            u.id,
            u.codigo_usuario,
            u.nome,
            u.nivel,
            u.regional,
            u.ativo,
            u.email,
            u.telefone,
            u.supervisor_codigo,
            u.gerente_codigo,
            sup.nome AS supervisor_nome,
            ger.nome AS gerente_nome,
            u.criado_em,
            u.atualizado_em
          FROM usuarios u
          LEFT JOIN usuarios sup ON UPPER(COALESCE(sup.codigo_usuario, '')) = UPPER(COALESCE(u.supervisor_codigo, ''))
          LEFT JOIN usuarios ger ON UPPER(COALESCE(ger.codigo_usuario, '')) = UPPER(COALESCE(u.gerente_codigo, ''))
         WHERE UPPER(u.codigo_usuario) = UPPER(%s)
         LIMIT 1
    """, (codigo_usuario,))
    usuario = cur.fetchone()
    cur.close()
    conn.close()
    return usuario


def listar_supervisores_admin():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT codigo_usuario, nome
          FROM usuarios
         WHERE LOWER(COALESCE(nivel, '')) = 'supervisor'
           AND COALESCE(ativo, TRUE) = TRUE
         ORDER BY nome, codigo_usuario
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows or []


def listar_gerentes_admin():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT codigo_usuario, nome
          FROM usuarios
         WHERE LOWER(COALESCE(nivel, '')) = 'gerente'
           AND COALESCE(ativo, TRUE) = TRUE
         ORDER BY nome, codigo_usuario
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows or []


def montar_options_usuario_admin(usuarios, valor_atual: Optional[str] = None, placeholder: str = 'Selecione') -> str:
    atual = str(valor_atual or '').strip().upper()
    html = [f'<option value="">{escape(placeholder)}</option>']
    for u in usuarios or []:
        codigo = str(u.get('codigo_usuario') or '').strip().upper()
        nome = str(u.get('nome') or '').strip()
        selected = ' selected' if codigo and codigo == atual else ''
        label = f'{codigo} - {nome}' if nome else codigo
        html.append(f'<option value="{escape(codigo)}"{selected}>{escape(label)}</option>')
    return ''.join(html)


def salvar_usuario_admin(codigo_usuario: str, nome: str, nivel: str, regional: Optional[str],
                         email: Optional[str], telefone: Optional[str], ativo: bool,
                         nova_senha: Optional[str] = None,
                         supervisor_codigo: Optional[str] = None,
                         gerente_codigo: Optional[str] = None):
    codigo_usuario = str(codigo_usuario or '').strip().upper()
    nome = str(nome or '').strip()
    nivel = str(nivel or '').strip().lower()
    regional = texto_limpo(regional)
    email = texto_limpo(email)
    telefone = texto_limpo(telefone)
    supervisor_codigo = texto_limpo(supervisor_codigo)
    gerente_codigo = texto_limpo(gerente_codigo)
    supervisor_codigo = str(supervisor_codigo or '').strip().upper() or None
    gerente_codigo = str(gerente_codigo or '').strip().upper() or None
    senha_hash = gerar_hash_senha(nova_senha) if texto_limpo(nova_senha) else None

    if not codigo_usuario or not nome or not nivel:
        raise ValueError('Informe código, nome e perfil do usuário.')

    if nivel == 'atendente' and not supervisor_codigo:
        raise ValueError('Informe o supervisor do atendente.')
    if nivel == 'supervisor' and not gerente_codigo:
        raise ValueError('Informe o gerente do supervisor.')

    if nivel != 'atendente':
        supervisor_codigo = None
    if nivel != 'supervisor':
        gerente_codigo = None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT id FROM usuarios WHERE UPPER(codigo_usuario) = UPPER(%s) LIMIT 1', (codigo_usuario,))
    existente = cur.fetchone()

    if existente:
        if senha_hash:
            cur.execute("""
                UPDATE usuarios
                   SET nome = %s,
                       nivel = %s,
                       regional = %s,
                       ativo = %s,
                       email = %s,
                       telefone = %s,
                       supervisor_codigo = %s,
                       gerente_codigo = %s,
                       senha_hash = %s,
                       atualizado_em = CURRENT_TIMESTAMP
                 WHERE id = %s
            """, (nome, nivel, regional, ativo, email, telefone, supervisor_codigo, gerente_codigo, senha_hash, existente[0]))
        else:
            cur.execute("""
                UPDATE usuarios
                   SET nome = %s,
                       nivel = %s,
                       regional = %s,
                       ativo = %s,
                       email = %s,
                       telefone = %s,
                       supervisor_codigo = %s,
                       gerente_codigo = %s,
                       atualizado_em = CURRENT_TIMESTAMP
                 WHERE id = %s
            """, (nome, nivel, regional, ativo, email, telefone, supervisor_codigo, gerente_codigo, existente[0]))
    else:
        cur.execute("""
            INSERT INTO usuarios (
                codigo_usuario, nome, senha_hash, nivel, regional, ativo, email, telefone,
                supervisor_codigo, gerente_codigo, criado_em, atualizado_em
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (codigo_usuario, nome, senha_hash, nivel, regional, ativo, email, telefone, supervisor_codigo, gerente_codigo))

    conn.commit()
    cur.close()
    conn.close()


def atualizar_senha_usuario_admin(codigo_usuario: str, nova_senha: str):
    codigo_usuario = str(codigo_usuario or '').strip().upper()
    nova_senha = str(nova_senha or '').strip()
    if not codigo_usuario or not nova_senha:
        raise ValueError('Informe o usuário e a nova senha.')
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE usuarios
           SET senha_hash = %s,
               atualizado_em = CURRENT_TIMESTAMP
         WHERE UPPER(codigo_usuario) = UPPER(%s)
    """, (gerar_hash_senha(nova_senha), codigo_usuario))
    if cur.rowcount <= 0:
        conn.rollback()
        cur.close()
        conn.close()
        raise ValueError('Usuário não encontrado para alterar a senha.')
    conn.commit()
    cur.close()
    conn.close()


def montar_options_perfil_admin(valor_atual: Optional[str] = None) -> str:
    perfis = [
        ('admin', 'Admin'),
        ('atendente', 'Atendente'),
        ('supervisor', 'Supervisor'),
        ('gerente', 'Gerente'),
        ('diretor', 'Diretor'),
        ('supervisor_financeiro', 'Supervisor Financeiro'),
        ('financeiro', 'Financeiro'),
    ]
    atual = (valor_atual or '').strip().lower()
    html = []
    for valor, label in perfis:
        selected = ' selected' if valor == atual else ''
        html.append(f'<option value="{valor}"{selected}>{label}</option>')
    return ''.join(html)


@app.route("/admin/usuarios", methods=["GET", "POST"])
@login_required
@role_required("admin")
def admin_usuarios():
    if request.method == 'POST':
        acao = (request.form.get('acao') or '').strip().lower()
        try:
            if acao == 'salvar_usuario':
                salvar_usuario_admin(
                    codigo_usuario=request.form.get('codigo_usuario') or '',
                    nome=request.form.get('nome') or '',
                    nivel=request.form.get('nivel') or '',
                    regional=request.form.get('regional'),
                    email=request.form.get('email'),
                    telefone=request.form.get('telefone'),
                    ativo=bool(request.form.get('ativo')),
                    nova_senha=request.form.get('nova_senha'),
                    supervisor_codigo=request.form.get('supervisor_codigo'),
                    gerente_codigo=request.form.get('gerente_codigo'),
                )
                flash('Usuário salvo com sucesso.', 'success')
            elif acao == 'alterar_senha':
                atualizar_senha_usuario_admin(
                    codigo_usuario=request.form.get('codigo_usuario') or '',
                    nova_senha=request.form.get('nova_senha') or '',
                )
                flash('Senha atualizada com sucesso.', 'success')
        except Exception as e:
            flash(f'Erro ao salvar usuário: {e}', 'error')
        return redirect(url_for('admin_usuarios', edit=request.form.get('codigo_usuario') or ''))

    codigo_edicao = (request.args.get('edit') or '').strip()
    usuario_edicao = obter_usuario_admin(codigo_edicao) if codigo_edicao else None
    usuarios = listar_usuarios_admin()
    supervisores = listar_supervisores_admin()
    gerentes = listar_gerentes_admin()

    linhas = []
    for u in usuarios:
        codigo_raw = str(u.get('codigo_usuario') or '')
        codigo_u = escape(codigo_raw or '-')
        nome_u = escape(str(u.get('nome') or '-'))
        nivel_u = escape(str(u.get('nivel') or '-'))
        regional_u = escape(str(u.get('regional') or '-'))
        email_u = escape(str(u.get('email') or '-'))
        telefone_u = escape(str(u.get('telefone') or '-'))
        supervisor_u = escape(str(u.get('supervisor_nome') or u.get('supervisor_codigo') or '-'))
        gerente_u = escape(str(u.get('gerente_nome') or u.get('gerente_codigo') or '-'))
        badge_ativo = '<span class="badge badge-success">Ativo</span>' if u.get('ativo') else '<span class="badge badge-danger">Inativo</span>'
        linhas.append(
            f"<tr>"
            f"<td><strong>{nome_u}</strong><div class='small-muted'>{codigo_u}</div></td>"
            f"<td>{nivel_u.capitalize()}</td>"
            f"<td>{regional_u}</td>"
            f"<td>{supervisor_u}</td>"
            f"<td>{gerente_u}</td>"
            f"<td>{email_u}</td>"
            f"<td>{telefone_u}</td>"
            f"<td>{badge_ativo}</td>"
            f"<td><a class='btn btn-outline' href='/admin/usuarios?edit={escape(codigo_raw)}'>Editar</a></td>"
            f"</tr>"
        )

    user = usuario_edicao or {}
    codigo_val = valor_input_texto(user.get('codigo_usuario', ''))
    nome_val = valor_input_texto(user.get('nome', ''))
    nivel_val = user.get('nivel', 'atendente')
    regional_val = valor_input_texto(user.get('regional', ''))
    email_val = valor_input_texto(user.get('email', ''))
    telefone_val = valor_input_texto(user.get('telefone', ''))
    supervisor_val = valor_input_texto(user.get('supervisor_codigo', ''))
    gerente_val = valor_input_texto(user.get('gerente_codigo', ''))
    ativo_checked = 'checked' if (user.get('ativo', True) if usuario_edicao else True) else ''
    readonly_attr = 'readonly' if usuario_edicao else ''
    senha_required_attr = '' if usuario_edicao else 'required'
    senha_label = 'Nova senha (opcional)' if usuario_edicao else 'Senha inicial'
    senha_placeholder = 'Preencha para trocar a senha' if usuario_edicao else 'Informe a senha inicial'
    titulo_form = 'Editar usuário' if usuario_edicao else 'Novo usuário'
    subtitulo_form = 'Atualize contatos, perfil, regional, vínculo hierárquico e senha do usuário.' if usuario_edicao else 'Cadastre usuários, defina senha inicial e configure os vínculos hierárquicos.'
    bloco_senha = ''
    if usuario_edicao:
        nome_senha = escape(str(user.get('nome') or user.get('codigo_usuario') or 'usuário'))
        bloco_senha = f"""
        <div class="panel">
            <div class="table-toolbar">
                <h3>Redefinir senha de {nome_senha}</h3>
                <div class="hint">Use este bloco quando quiser trocar a senha sem alterar os demais dados.</div>
            </div>
            <form method="post">
                <input type="hidden" name="acao" value="alterar_senha">
                <input type="hidden" name="codigo_usuario" value="{codigo_val}">
                <div class="grid-2">
                    <div class="field"><label>Usuário</label><input type="text" value="{codigo_val}" readonly></div>
                    <div class="field"><label>Nova senha</label><input type="password" name="nova_senha" placeholder="Informe a nova senha" required></div>
                </div>
                <p style="margin-top:14px;"><button class="btn" type="submit">Atualizar senha</button></p>
            </form>
        </div>
        """

    content = f"""
    <div class="page-head">
        <div class="page-title">Usuários</div>
        <div class="page-subtitle">Somente o admin pode cadastrar, editar contatos, vínculos hierárquicos e redefinir senhas.</div>
    </div>

    <div class="panel">
        <div class="table-toolbar">
            <h3>{titulo_form}</h3>
            <div class="hint">{subtitulo_form}</div>
        </div>
        <form method="post">
            <input type="hidden" name="acao" value="salvar_usuario">
            <div class="grid-2">
                <div class="field"><label>Código do usuário</label><input type="text" name="codigo_usuario" value="{codigo_val}" placeholder="Ex.: ADM001" required {readonly_attr}></div>
                <div class="field"><label>Nome</label><input type="text" name="nome" value="{nome_val}" placeholder="Nome completo" required></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>Perfil</label><select name="nivel" id="nivel_usuario" required>{montar_options_perfil_admin(nivel_val)}</select></div>
                <div class="field"><label>Regional</label><input type="text" name="regional" value="{regional_val}" placeholder="Ex.: NORTE / NORDESTE"></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>E-mail</label><input type="email" name="email" value="{email_val}" placeholder="email@empresa.com"></div>
                <div class="field"><label>Telefone</label><input type="text" name="telefone" value="{telefone_val}" placeholder="Ex.: 5518999999999"></div>
            </div>
            <div class="grid-2">
                <div class="field" id="field_supervisor"><label>Supervisor do atendente</label><select name="supervisor_codigo">{montar_options_usuario_admin(supervisores, supervisor_val, 'Selecione o supervisor')}</select></div>
                <div class="field" id="field_gerente"><label>Gerente do supervisor</label><select name="gerente_codigo">{montar_options_usuario_admin(gerentes, gerente_val, 'Selecione o gerente')}</select></div>
            </div>
            <div class="grid-2">
                <div class="field"><label>{senha_label}</label><input type="password" name="nova_senha" placeholder="{senha_placeholder}" {senha_required_attr}></div>
                <div class="field"><label><input type="checkbox" name="ativo" {ativo_checked}> Usuário ativo</label></div>
            </div>
            <p style="margin-top:14px; display:flex; gap:10px; flex-wrap:wrap;"><button class="btn" type="submit">Salvar usuário</button><a class="btn btn-outline" href="/admin/usuarios">Limpar</a></p>
        </form>
    </div>

    {bloco_senha}

    <div class="panel">
        <div class="table-toolbar">
            <h3>Usuários cadastrados</h3>
            <div class="hint">Edite perfis, regionais, vínculos, e-mail, telefone e status diretamente por aqui.</div>
        </div>
        <div class="saf-table-wrap">
            <table class="saf-table">
                <thead>
                    <tr>
                        <th>Usuário</th><th>Perfil</th><th>Regional</th><th>Supervisor</th><th>Gerente</th><th>E-mail</th><th>Telefone</th><th>Status</th><th>Ações</th>
                    </tr>
                </thead>
                <tbody>{''.join(linhas) if linhas else '<tr><td colspan="9">Nenhum usuário cadastrado.</td></tr>'}</tbody>
            </table>
        </div>
    </div>

    <script>
        function atualizarCamposVinculoUsuario() {{
            const nivel = (document.getElementById('nivel_usuario')?.value || '').toLowerCase();
            const fieldSupervisor = document.getElementById('field_supervisor');
            const fieldGerente = document.getElementById('field_gerente');
            const selectSupervisor = fieldSupervisor ? fieldSupervisor.querySelector('select') : null;
            const selectGerente = fieldGerente ? fieldGerente.querySelector('select') : null;

            if (fieldSupervisor) fieldSupervisor.style.display = (nivel === 'atendente') ? '' : 'none';
            if (fieldGerente) fieldGerente.style.display = (nivel === 'supervisor') ? '' : 'none';

            if (selectSupervisor) {{
                selectSupervisor.required = (nivel === 'atendente');
                if (nivel !== 'atendente') selectSupervisor.value = '';
            }}
            if (selectGerente) {{
                selectGerente.required = (nivel === 'supervisor');
                if (nivel !== 'supervisor') selectGerente.value = '';
            }}
        }}

        document.addEventListener('DOMContentLoaded', function() {{
            const nivel = document.getElementById('nivel_usuario');
            if (nivel) {{
                nivel.addEventListener('change', atualizarCamposVinculoUsuario);
            }}
            atualizarCamposVinculoUsuario();
        }});
    </script>
    """
    return render_base(content, "Usuários | SAF")


@app.errorhandler(403)
def forbidden(_error):
    return render_template_string("""
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <title>Acesso negado</title>
        <link rel="icon" type="image/x-icon" href="/logo-kidy-icon">
        <style>
            body {
                background: #fff7ed;
                color: #111827;
                font-family: Arial, Helvetica, sans-serif;
                display: flex;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                margin: 0;
                padding: 24px;
            }
            .box {
                background: #ffffff;
                border: 1px solid #fdba74;
                border-radius: 22px;
                padding: 32px;
                text-align: center;
                max-width: 440px;
                box-shadow: 0 20px 50px rgba(154,52,18,0.12);
            }
            .logo {
                width: 180px;
                max-width: 100%;
                display: block;
                margin: 0 auto 18px auto;
            }
            h1 {
                color: #9a3412;
                margin-bottom: 10px;
            }
            p {
                color: #374151;
            }
            a {
                display: inline-block;
                margin-top: 18px;
                color: #ffffff;
                background: #f97316;
                text-decoration: none;
                padding: 12px 18px;
                border-radius: 14px;
                font-weight: 700;
            }
            a:hover {
                background: #ea580c;
            }
        </style>
    </head>
    <body>
        <div class="box">
            <img class="logo" src="/logo-kidy" alt="Logo Kidy">
            <h1>Acesso negado</h1>
            <p>Você não tem permissão para acessar esta área.</p>
            <a href="/dashboard">Voltar ao dashboard</a>
        </div>
    </body>
    </html>
    """), 403

if __name__ == "__main__":
    print("=== APP CERTO EM EXECUCAO: backend/app/app.py ===")
    app.run(debug=True)
