from sqlalchemy.orm import Session
from sqlalchemy import text
from types import SimpleNamespace
from passlib.exc import UnknownHashError

from app.core.security import verificar_senha


def login(db: Session, codigo_usuario: str, senha: str):
    codigo_usuario = (codigo_usuario or "").strip()
    senha = (senha or "").strip()

    print(f"LOGIN DEBUG -> codigo recebido: [{codigo_usuario}]")
    print(f"LOGIN DEBUG -> senha recebida: [{senha}]")

    # prova de qual banco/schema/tabela a aplicação está lendo
    debug_query = text("""
        SELECT
            current_database() AS banco,
            current_schema() AS schema_atual,
            COUNT(*) AS total_usuarios
        FROM usuarios
    """)
    debug_info = db.execute(debug_query).mappings().first()
    print(f"LOGIN DEBUG -> banco={debug_info['banco']} schema={debug_info['schema_atual']} total_usuarios={debug_info['total_usuarios']}")

    lista_query = text("""
        SELECT id, codigo_usuario, nome
        FROM usuarios
        ORDER BY id
    """)
    lista = db.execute(lista_query).mappings().all()
    print("LOGIN DEBUG -> usuarios visiveis pela app:")
    for item in lista:
        print(f"   id={item['id']} codigo=[{item['codigo_usuario']}] nome=[{item['nome']}]")

    query = text("""
        SELECT
            id,
            codigo_usuario,
            nome,
            ativo,
            senha_hash
        FROM usuarios
        WHERE UPPER(TRIM(codigo_usuario)) = UPPER(TRIM(:codigo_usuario))
        LIMIT 1
    """)

    row = db.execute(
        query,
        {"codigo_usuario": codigo_usuario}
    ).mappings().first()

    if not row:
        print(f"LOGIN DEBUG -> usuário NÃO encontrado no banco: [{codigo_usuario}]")
        return None

    print(
        f"LOGIN DEBUG -> usuário encontrado: "
        f"id={row['id']}, codigo={row['codigo_usuario']}, nome={row['nome']}"
    )

    if "ativo" in row and not row["ativo"]:
        print(f"LOGIN DEBUG -> usuário inativo: [{codigo_usuario}]")
        return None

    try:
        ok = verificar_senha(senha, row["senha_hash"])
        print(f"LOGIN DEBUG -> resultado verificar_senha: {ok}")

        if not ok:
            print(f"LOGIN DEBUG -> senha inválida para [{codigo_usuario}]")
            return None

    except UnknownHashError:
        print(f"LOGIN DEBUG -> hash inválido no banco para [{codigo_usuario}] -> {row['senha_hash']}")
        return None
    except Exception as e:
        print(f"LOGIN DEBUG -> erro inesperado: {e}")
        return None

    print(f"LOGIN DEBUG -> LOGIN OK para [{codigo_usuario}]")

    return SimpleNamespace(
        id=row["id"],
        codigo_usuario=row["codigo_usuario"],
        nome=row["nome"],
        ativo=row["ativo"],
        senha_hash=row["senha_hash"],
    )