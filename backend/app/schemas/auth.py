from pydantic import BaseModel

class LoginRequest(BaseModel):
    codigo_usuario: str
    senha: str