from sqlalchemy import Column, Integer, String, Boolean
from app.db.base import Base


class Usuario(Base):
    __tablename__ = "usuarios"

    id = Column(Integer, primary_key=True, index=True)
    codigo_usuario = Column(String, unique=True, index=True)
    nome = Column(String)
    email = Column(String)
    senha_hash = Column(String)
    nivel = Column(String)
    ativo = Column(Boolean, default=True)