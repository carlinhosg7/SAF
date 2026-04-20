from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

from app.core.config import settings

DATABASE_URL = settings.DATABASE_URL
print(f"DATABASE_URL CARREGADA = {DATABASE_URL!r}")

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()


def testar_conexao():
    try:
        with engine.connect() as conn:
            resultado = conn.execute(text("""
                SELECT
                    current_database() AS banco,
                    current_schema() AS schema_atual,
                    inet_server_addr() AS server_addr,
                    inet_server_port() AS server_port,
                    current_user AS usuario
            """)).mappings().first()

            print("DEBUG CONEXAO BANCO ->", dict(resultado))

            usuarios = conn.execute(text("""
                SELECT id, codigo_usuario, nome
                FROM usuarios
                ORDER BY id
            """)).mappings().all()

            print("DEBUG USUARIOS VISIVEIS NA CONEXAO:")
            for u in usuarios:
                print(dict(u))

    except Exception as e:
        print("ERRO AO TESTAR CONEXAO:", e)