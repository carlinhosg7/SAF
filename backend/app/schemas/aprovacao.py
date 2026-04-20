from pydantic import BaseModel

class AprovarRequest(BaseModel):
    saf_id: int
    nivel: int
    usuario_id: int
    nome: str
    codigo: str
    status: str