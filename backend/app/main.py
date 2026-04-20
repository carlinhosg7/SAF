from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware

import app.api.v1.auth_routes as auth_routes
import app.api.v1.saf_routes as saf_routes
import app.api.v1.aprovacao_routes as aprovacao_routes
import app.api.v1.web_routes as web_routes

from app.db.session import testar_conexao

app = FastAPI(title="SAF API")

app.add_middleware(SessionMiddleware, secret_key="saf_secret_key_123")

app.include_router(auth_routes.router, prefix="/auth", tags=["Auth"])
app.include_router(saf_routes.router, prefix="/saf", tags=["SAF"])
app.include_router(aprovacao_routes.router)
app.include_router(web_routes.router)


@app.on_event("startup")
def startup_event():
    testar_conexao()


@app.get("/")
def root():
    return {"message": "SAF rodando"}