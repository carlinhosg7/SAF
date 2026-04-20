from functools import wraps
from flask import session, redirect, abort

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

def role_required(*roles):
    def wrapper(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if "role" not in session or session["role"] not in roles:
                abort(403)
            return f(*args, **kwargs)
        return decorated
    return wrapper