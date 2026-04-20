from database.connection import get_conn

def get_kpis(rep=None):
    conn = get_conn()
    cur = conn.cursor()

    # TOTAL CLIENTES
    if rep:
        cur.execute("SELECT COUNT(*) FROM clientes WHERE representante=%s", (rep,))
    else:
        cur.execute("SELECT COUNT(*) FROM clientes")
    total_clientes = cur.fetchone()[0]

    # ATENDIMENTOS HOJE
    if rep:
        cur.execute("""
            SELECT COUNT(*) FROM atendimentos
            WHERE representante=%s
            AND DATE(data_atendimento) = CURRENT_DATE
        """, (rep,))
    else:
        cur.execute("""
            SELECT COUNT(*) FROM atendimentos
            WHERE DATE(data_atendimento) = CURRENT_DATE
        """)
    atend_hoje = cur.fetchone()[0]

    # CLIENTES SEM ATENDIMENTO
    if rep:
        cur.execute("""
            SELECT COUNT(*) FROM clientes c
            WHERE representante=%s
            AND NOT EXISTS (
                SELECT 1 FROM atendimentos a WHERE a.cliente_id = c.id
            )
        """, (rep,))
    else:
        cur.execute("""
            SELECT COUNT(*) FROM clientes c
            WHERE NOT EXISTS (
                SELECT 1 FROM atendimentos a WHERE a.cliente_id = c.id
            )
        """)
    sem_atendimento = cur.fetchone()[0]

    # CONVERSÃO
    if rep:
        cur.execute("""
            SELECT COUNT(*) FROM atendimentos
            WHERE representante=%s AND status='Pedido realizado'
        """, (rep,))
        pedidos = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM atendimentos WHERE representante=%s", (rep,))
        total_atend = cur.fetchone()[0]
    else:
        cur.execute("SELECT COUNT(*) FROM atendimentos WHERE status='Pedido realizado'")
        pedidos = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM atendimentos")
        total_atend = cur.fetchone()[0]

    conversao = (pedidos / total_atend * 100) if total_atend > 0 else 0

    cur.close()
    conn.close()

    return {
        "clientes": total_clientes,
        "atendimentos_hoje": atend_hoje,
        "sem_atendimento": sem_atendimento,
        "conversao": round(conversao, 2)
    }


def ultimos_atendimentos(rep=None):
    conn = get_conn()
    cur = conn.cursor()

    if rep:
        cur.execute("""
            SELECT c.nome, a.data_atendimento, a.status
            FROM atendimentos a
            JOIN clientes c ON c.id = a.cliente_id
            WHERE a.representante=%s
            ORDER BY a.data_atendimento DESC
            LIMIT 10
        """, (rep,))
    else:
        cur.execute("""
            SELECT c.nome, a.data_atendimento, a.status
            FROM atendimentos a
            JOIN clientes c ON c.id = a.cliente_id
            ORDER BY a.data_atendimento DESC
            LIMIT 10
        """)

    dados = cur.fetchall()
    cur.close()
    conn.close()

    return dados