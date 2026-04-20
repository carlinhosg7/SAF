from flask import Blueprint, request, session, redirect, flash, render_template_string
from database.connection import get_conn
import bcrypt

auth_bp = Blueprint("auth", __name__)

def get_role(codigo):
    codigo = (codigo or "").upper()

    if codigo.startswith("ATD"):
        return "atendente"
    elif codigo.startswith("COR"):
        return "coordenador"
    elif codigo.startswith("GER"):
        return "gerente"
    elif codigo.startswith("DIR"):
        return "diretor"
    elif codigo.startswith("ADM"):
        return "admin"
    return "usuario"


@auth_bp.route("/", methods=["GET"])
def home():
    if "user_id" in session:
        return redirect("/dashboard")
    return redirect("/login")


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    erro = None

    if request.method == "POST":
        codigo = request.form.get("codigo", "").strip().upper()
        senha = request.form.get("senha", "").strip()

        if not codigo or not senha:
            erro = "Preencha código e senha."
        else:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute(
                "SELECT id, codigo_usuario, nome, senha_hash FROM usuarios WHERE UPPER(codigo_usuario) = %s",
                (codigo,)
            )
            user = cur.fetchone()

            cur.close()
            conn.close()

            if user:
                user_id, cod, nome, senha_hash = user

                senha_hash_str = str(senha_hash)

                if bcrypt.checkpw(senha.encode("utf-8"), senha_hash_str.encode("utf-8")):
                    session["user_id"] = user_id
                    session["codigo"] = cod
                    session["nome"] = nome
                    session["role"] = get_role(cod)

                    flash("Login realizado com sucesso.", "success")
                    return redirect("/dashboard")

            erro = "Código ou senha inválidos."

    html = """
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>SAF | Login</title>
        <style>
            * {
                box-sizing: border-box;
                margin: 0;
                padding: 0;
                font-family: Arial, Helvetica, sans-serif;
            }

            body {
                min-height: 100vh;
                background:
                    radial-gradient(circle at top left, #1f4fff 0%, transparent 30%),
                    radial-gradient(circle at bottom right, #00b894 0%, transparent 28%),
                    linear-gradient(135deg, #0b1020 0%, #111827 45%, #0f172a 100%);
                color: #e5e7eb;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 24px;
            }

            .login-wrap {
                width: 100%;
                max-width: 980px;
                display: grid;
                grid-template-columns: 1.1fr 0.9fr;
                background: rgba(17, 24, 39, 0.88);
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 24px;
                overflow: hidden;
                box-shadow: 0 25px 60px rgba(0,0,0,0.45);
                backdrop-filter: blur(10px);
            }

            .brand {
                padding: 48px;
                background:
                    linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.01)),
                    linear-gradient(135deg, #111827 0%, #0f172a 100%);
                display: flex;
                flex-direction: column;
                justify-content: center;
            }

            .brand-badge {
                display: inline-flex;
                align-items: center;
                gap: 10px;
                width: fit-content;
                padding: 8px 14px;
                border-radius: 999px;
                background: rgba(59, 130, 246, 0.14);
                border: 1px solid rgba(96, 165, 250, 0.20);
                color: #93c5fd;
                font-size: 13px;
                margin-bottom: 18px;
            }

            .brand h1 {
                font-size: 42px;
                line-height: 1.05;
                color: #ffffff;
                margin-bottom: 14px;
            }

            .brand p {
                color: #94a3b8;
                font-size: 16px;
                line-height: 1.7;
                max-width: 460px;
            }

            .brand-grid {
                display: grid;
                grid-template-columns: repeat(2, 1fr);
                gap: 14px;
                margin-top: 28px;
            }

            .mini-card {
                background: rgba(255,255,255,0.04);
                border: 1px solid rgba(255,255,255,0.05);
                border-radius: 16px;
                padding: 16px;
            }

            .mini-card strong {
                display: block;
                color: #fff;
                font-size: 15px;
                margin-bottom: 6px;
            }

            .mini-card span {
                color: #94a3b8;
                font-size: 13px;
                line-height: 1.5;
            }

            .panel {
                padding: 42px 34px;
                display: flex;
                align-items: center;
                justify-content: center;
                background: rgba(15, 23, 42, 0.82);
            }

            .card {
                width: 100%;
                max-width: 380px;
            }

            .card h2 {
                color: #ffffff;
                font-size: 28px;
                margin-bottom: 8px;
            }

            .card .sub {
                color: #94a3b8;
                margin-bottom: 26px;
                font-size: 14px;
            }

            .alert {
                width: 100%;
                margin-bottom: 18px;
                padding: 12px 14px;
                border-radius: 12px;
                font-size: 14px;
                font-weight: 600;
            }

            .alert-error {
                background: rgba(239, 68, 68, 0.12);
                color: #fecaca;
                border: 1px solid rgba(239, 68, 68, 0.28);
            }

            .form-group {
                margin-bottom: 16px;
            }

            .form-group label {
                display: block;
                margin-bottom: 8px;
                color: #cbd5e1;
                font-size: 14px;
                font-weight: 600;
            }

            .input {
                width: 100%;
                height: 48px;
                border-radius: 14px;
                border: 1px solid rgba(255,255,255,0.10);
                background: rgba(255,255,255,0.04);
                color: #fff;
                padding: 0 14px;
                font-size: 15px;
                outline: none;
                transition: 0.2s ease;
            }

            .input::placeholder {
                color: #64748b;
            }

            .input:focus {
                border-color: rgba(96, 165, 250, 0.85);
                box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.16);
                background: rgba(255,255,255,0.06);
            }

            .btn {
                width: 100%;
                height: 50px;
                border: none;
                border-radius: 14px;
                background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
                color: #fff;
                font-size: 15px;
                font-weight: 700;
                cursor: pointer;
                transition: transform 0.15s ease, box-shadow 0.15s ease, opacity 0.15s ease;
                box-shadow: 0 12px 25px rgba(37, 99, 235, 0.30);
            }

            .btn:hover {
                transform: translateY(-1px);
                opacity: 0.96;
            }

            .footer-note {
                margin-top: 16px;
                text-align: center;
                color: #64748b;
                font-size: 12px;
            }

            @media (max-width: 900px) {
                .login-wrap {
                    grid-template-columns: 1fr;
                }

                .brand {
                    display: none;
                }

                .panel {
                    padding: 30px 20px;
                }
            }
        </style>
    </head>
    <body>
        <div class="login-wrap">
            <section class="brand">
                <div class="brand-badge">● Sistema SAF</div>
                <h1>Gestão comercial com visão operacional e estratégica</h1>
                <p>
                    Acompanhe clientes, atendimentos, prioridades e indicadores em um único ambiente.
                    Seu SAF já está rodando; agora ele também vai ficar bonito.
                </p>

                <div class="brand-grid">
                    <div class="mini-card">
                        <strong>Clientes</strong>
                        <span>Cadastro, carteira e visão centralizada.</span>
                    </div>
                    <div class="mini-card">
                        <strong>Atendimentos</strong>
                        <span>Histórico e produtividade por usuário.</span>
                    </div>
                    <div class="mini-card">
                        <strong>Prioridades</strong>
                        <span>Lista inteligente para ação comercial.</span>
                    </div>
                    <div class="mini-card">
                        <strong>Dashboard</strong>
                        <span>Indicadores e acompanhamento em tempo real.</span>
                    </div>
                </div>
            </section>

            <section class="panel">
                <div class="card">
                    <h2>Entrar no SAF</h2>
                    <p class="sub">Acesse com seu código de usuário e senha.</p>

                    {% if erro %}
                        <div class="alert alert-error">{{ erro }}</div>
                    {% endif %}

                    <form method="post">
                        <div class="form-group">
                            <label for="codigo">Código</label>
                            <input
                                class="input"
                                id="codigo"
                                name="codigo"
                                type="text"
                                placeholder="Ex.: ADM001"
                                autocomplete="username"
                                required
                            >
                        </div>

                        <div class="form-group">
                            <label for="senha">Senha</label>
                            <input
                                class="input"
                                id="senha"
                                name="senha"
                                type="password"
                                placeholder="Digite sua senha"
                                autocomplete="current-password"
                                required
                            >
                        </div>

                        <button class="btn" type="submit">Entrar</button>
                    </form>

                    <div class="footer-note">
                        SAF • Sistema de Acompanhamento e Força comercial
                    </div>
                </div>
            </section>
        </div>
    </body>
    </html>
    """

    return render_template_string(html, erro=erro)


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect("/login")