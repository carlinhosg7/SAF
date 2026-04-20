import bcrypt

usuarios = {
    "ATD001": "123",
    "COR001": "123",
    "GER001": "123",
    "DIR001": "123",
    "ADM001": "123",
}

for codigo, senha in usuarios.items():
    senha_hash = bcrypt.hashpw(senha.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    print(codigo, "->", senha_hash)