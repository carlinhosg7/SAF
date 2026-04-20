from database.connection import get_conn

def listar_clientes(rep=None):
    conn = get_conn()
    cur = conn.cursor()

    if rep:
        cur.execute("SELECT id, codigo, nome, cidade, uf FROM clientes WHERE representante=%s ORDER BY nome", (rep,))
    else:
        cur.execute("SELECT id, codigo, nome, cidade, uf FROM clientes ORDER BY nome")

    dados = cur.fetchall()
    cur.close()
    conn.close()

    return dados


def inserir_cliente(codigo, nome, cidade, uf, representante):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO clientes (codigo, nome, cidade, uf, representante)
        VALUES (%s, %s, %s, %s, %s)
    """, (codigo, nome, cidade, uf, representante))

    conn.commit()
    cur.close()
    conn.close()

def clientes_sem_atendimento(rep=None):
    conn = get_conn()
    cur = conn.cursor()

    if rep:
        cur.execute("""
            SELECT c.id, c.nome, c.cidade, c.uf
            FROM clientes c
            WHERE representante=%s
            AND NOT EXISTS (
                SELECT 1 FROM atendimentos a WHERE a.cliente_id = c.id
            )
            ORDER BY c.nome
        """, (rep,))
    else:
        cur.execute("""
            SELECT c.id, c.nome, c.cidade, c.uf
            FROM clientes c
            WHERE NOT EXISTS (
                SELECT 1 FROM atendimentos a WHERE a.cliente_id = c.id
            )
            ORDER BY c.nome
        """)

    dados = cur.fetchall()
    cur.close()
    conn.close()

    return dados

def clientes_com_prioridade(rep=None):
    conn = get_conn()
    cur = conn.cursor()

    if rep:
        cur.execute("""
            SELECT 
                c.id,
                c.nome,
                c.cidade,
                c.uf,
                MAX(a.data_atendimento) as ultima_data
            FROM clientes c
            LEFT JOIN atendimentos a ON a.cliente_id = c.id
            WHERE c.representante=%s
            GROUP BY c.id, c.nome, c.cidade, c.uf
        """, (rep,))
    else:
        cur.execute("""
            SELECT 
                c.id,
                c.nome,
                c.cidade,
                c.uf,
                MAX(a.data_atendimento) as ultima_data
            FROM clientes c
            LEFT JOIN atendimentos a ON a.cliente_id = c.id
            GROUP BY c.id, c.nome, c.cidade, c.uf
        """)

    dados = cur.fetchall()
    cur.close()
    conn.close()

    resultado = []

    from datetime import datetime

    for c in dados:
        ultima = c[4]

        if not ultima:
            prioridade = "🔴 ALTA"
        else:
            dias = (datetime.now() - ultima).days

            if dias > 30:
                prioridade = "🟡 MÉDIA"
            else:
                prioridade = "🟢 BAIXA"

        resultado.append((c[0], c[1], c[2], c[3], prioridade))

    return resultado