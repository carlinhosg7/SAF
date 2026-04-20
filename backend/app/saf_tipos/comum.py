# -*- coding: utf-8 -*-
from html import escape


def e(valor):
    return "" if valor is None else escape(str(valor))


def option(valor, label, selected=False):
    return f'<option value="{e(valor)}" {"selected" if selected else ""}>{e(label)}</option>'


def buscar_representante_cache(codigo_representante: str, get_conn_func):
    codigo_representante = (codigo_representante or "").strip()
    if not codigo_representante:
        return None

    conn = get_conn_func()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COALESCE(codigo_representante, ''),
            COALESCE(representante, ''),
            COALESCE(supervisor, '')
        FROM cache_clientes
        WHERE TRIM(COALESCE(codigo_representante, '')) = %s
        ORDER BY razao_social, codigo_cliente
        LIMIT 1
    """, (codigo_representante,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "codigo_representante": row[0] or "",
        "representante": row[1] or "",
        "supervisor": row[2] or "",
    }


def buscar_clientes_por_representante(codigo_representante: str, termo: str, get_conn_func):
    codigo_representante = (codigo_representante or "").strip()
    termo = (termo or "").strip()

    if not codigo_representante:
        return []

    conn = get_conn_func()
    cur = conn.cursor()

    if termo:
        like = f"%{termo}%"
        cur.execute("""
            SELECT
                COALESCE(codigo_cliente, '') AS codigo_cliente,
                COALESCE(razao_social, '') AS razao_social,
                COALESCE(cnpj, '') AS cnpj,
                COALESCE(codigo_grupo_cliente, '') AS codigo_grupo_cliente,
                COALESCE(grupo_cliente, '') AS grupo_cliente,
                COALESCE(codigo_representante, '') AS codigo_representante,
                COALESCE(representante, '') AS representante,
                COALESCE(supervisor, '') AS supervisor
            FROM cache_clientes
            WHERE TRIM(COALESCE(codigo_representante, '')) = %s
              AND (
                    COALESCE(codigo_cliente, '') ILIKE %s
                 OR COALESCE(razao_social, '') ILIKE %s
                 OR COALESCE(cnpj, '') ILIKE %s
                 OR COALESCE(codigo_grupo_cliente, '') ILIKE %s
                 OR COALESCE(grupo_cliente, '') ILIKE %s
              )
            ORDER BY razao_social, codigo_cliente
            LIMIT 300
        """, (codigo_representante, like, like, like, like, like))
    else:
        cur.execute("""
            SELECT
                COALESCE(codigo_cliente, '') AS codigo_cliente,
                COALESCE(razao_social, '') AS razao_social,
                COALESCE(cnpj, '') AS cnpj,
                COALESCE(codigo_grupo_cliente, '') AS codigo_grupo_cliente,
                COALESCE(grupo_cliente, '') AS grupo_cliente,
                COALESCE(codigo_representante, '') AS codigo_representante,
                COALESCE(representante, '') AS representante,
                COALESCE(supervisor, '') AS supervisor
            FROM cache_clientes
            WHERE TRIM(COALESCE(codigo_representante, '')) = %s
            ORDER BY razao_social, codigo_cliente
            LIMIT 300
        """, (codigo_representante,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "codigo_cliente": row[0] or "",
            "razao_social": row[1] or "",
            "cnpj": row[2] or "",
            "codigo_grupo_cliente": row[3] or "",
            "grupo_cliente": row[4] or "",
            "codigo_representante": row[5] or "",
            "representante": row[6] or "",
            "supervisor": row[7] or "",
        }
        for row in rows
    ]
