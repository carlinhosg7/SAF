from app.db import Base, engine, SessionLocal
from app.models import User
from app.auth import get_password_hash
from app.settings import settings

def main():
    Base.metadata.create_all(bind=engine)
    # cria admin se não existir
    with SessionLocal() as db:
        admin = db.query(User).filter(User.email == settings.ADMIN_EMAIL).first()
        if not admin:
            admin = User(
                name="Administrador",
                email=settings.ADMIN_EMAIL,
                role="ADMIN",
                password_hash=get_password_hash(settings.ADMIN_PASSWORD),
                active=True,
            )
            db.add(admin)
            db.commit()
            print("Usuário admin criado:", settings.ADMIN_EMAIL)
        else:
            print("Admin já existente:", settings.ADMIN_EMAIL)

if __name__ == "__main__":
    main()
