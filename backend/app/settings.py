from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    DATABASE_URL: str = Field(default="postgresql+psycopg://ocorrencias:ocorrencias@localhost:5432/ocorrencias")
    JWT_SECRET: str = Field(default="dev_secret_change_me")
    JWT_EXPIRES_MIN: int = Field(default=120)
    ADMIN_EMAIL: str = Field(default="admin@local")
    ADMIN_PASSWORD: str = Field(default="admin123")

    class Config:
        env_file = ".env"

settings = Settings()
