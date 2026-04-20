from database.connection import get_conn

def inserir_atendimento(cliente_id, representante, status, observacao):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO atendimentos (cliente_id, representante, status, observacao)
        VALUES (%s, %s, %s, %s)
    """, (cliente_id, representante, status, observacao))

    conn.commit()
    cur.close()
    conn.close()


def listar_atendimentos(rep=None):
    conn = get_conn()
    cur = conn.cursor()

    if rep:
        cur.execute("""
            SELECT a.id, c.nome, a.data_atendimento, a.status
            FROM atendimentos a
            JOIN clientes c ON c.id = a.cliente_id
            WHERE a.representante=%s
            ORDER BY a.data_atendimento DESC
        """, (rep,))
    else:
        cur.execute("""
            SELECT a.id, c.nome, a.data_atendimento, a.status
            FROM atendimentos a
            JOIN clientes c ON c.id = a.cliente_id
            ORDER BY a.data_atendimento DESC
        """)

    dados = cur.fetchall()
    cur.close()
    conn.close()

    return dados


def listar_clientes_select(rep=None):
    conn = get_conn()
    cur = conn.cursor()

    if rep:
        cur.execute("SELECT id, nome FROM clientes WHERE representante=%s ORDER BY nome", (rep,))
    else:
        cur.execute("SELECT id, nome FROM clientes ORDER BY nome")

    dados = cur.fetchall()
    cur.close()
    conn.close()

    return dados