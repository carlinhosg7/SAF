from sqlalchemy import String, Integer, Boolean, Text, ForeignKey, DateTime, Enum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime
from app.db import Base
import enum

class RoleEnum(str, enum.Enum):
    USER = "USER"
    MANAGER = "MANAGER"
    DIRECTOR = "DIRECTOR"
    ADMIN = "ADMIN"

class StatusEnum(str, enum.Enum):
    RASCUNHO = "RASCUNHO"
    ENVIADA = "ENVIADA"
    APROVADA_GERENCIA = "APROVADA_GERENCIA"
    APROVADA_DIRETORIA = "APROVADA_DIRETORIA"
    APROVADA = "APROVADA"
    REPROVADA = "REPROVADA"

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    email: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(20), default=RoleEnum.USER.value)
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    occurrences: Mapped[list["Occurrence"]] = relationship(back_populates="creator")

class Occurrence(Base):
    __tablename__ = "occurrences"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    title: Mapped[str] = mapped_column(String(200))
    description: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    status: Mapped[str] = mapped_column(String(30), default=StatusEnum.RASCUNHO.value)

    created_by: Mapped[int] = mapped_column(ForeignKey("users.id"))
    creator: Mapped[User] = relationship(back_populates="occurrences")

    attachments: Mapped[list["Attachment"]] = relationship(back_populates="occurrence", cascade="all, delete-orphan")
    approvals: Mapped[list["Approval"]] = relationship(back_populates="occurrence", cascade="all, delete-orphan")

class Attachment(Base):
    __tablename__ = "attachments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    filename: Mapped[str] = mapped_column(String(255))
    mime_type: Mapped[str] = mapped_column(String(100))
    path: Mapped[str] = mapped_column(String(300))
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    uploaded_by: Mapped[int] = mapped_column(ForeignKey("users.id"))

    occurrence_id: Mapped[int] = mapped_column(ForeignKey("occurrences.id"))
    occurrence: Mapped[Occurrence] = relationship(back_populates="attachments")

class Approval(Base):
    __tablename__ = "approvals"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    level: Mapped[str] = mapped_column(String(20))  # MANAGER / DIRECTOR
    decision: Mapped[str] = mapped_column(String(20))  # APPROVE / REJECT
    comment: Mapped[str] = mapped_column(Text, default="")
    decided_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    approver_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    occurrence_id: Mapped[int] = mapped_column(ForeignKey("occurrences.id"))

    occurrence: Mapped[Occurrence] = relationship(back_populates="approvals")
