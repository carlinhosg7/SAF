from pydantic import BaseModel
from typing import Optional, Dict, Any


class CriarSAF(BaseModel):
    codigo_saf: str
    tipo: str
    supervisor: str
    atendente: str
    usuario_id: int
    dados: Optional[Dict[str, Any]] = None