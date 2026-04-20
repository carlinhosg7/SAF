import bcrypt

senha = "123".encode()
hash = bcrypt.hashpw(senha, bcrypt.gensalt())

print(hash.decode())