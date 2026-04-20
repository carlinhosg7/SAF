from flask import Blueprint, request, redirect, session
from utils.decorators import login_required
from services.cliente_service import listar_clientes, inserir_cliente
from services.cliente_service import clientes_sem_atendimento
from services.cliente_service import clientes_com_prioridade


clientes_bp = Blueprint("clientes", __name__)

def render_base(content):
    menu = f"""
    <div style='width:220px;float:left;background:#111;color:#fff;height:100vh;padding:15px'>
        <h3>SAF</h3>
        <p>{session.get("nome")}</p>
        <hr>
        <a href="/dashboard" style="color:white">Dashboard</a><br>
        <a href="/clientes" style="color:white">Clientes</a><br>
        <a href="/clientes-inteligente" style="color:white">🔥 Prioridades</a><br>
        <a href="/atendimentos" style="color:white">Atendimentos</a><br>
        <a href="/relatorios" style="color:white">Relatórios</a><br>
        <a href="/logout" style="color:red">Sair</a>
    </div>
    <div style='margin-left:240px;padding:20px'>
        {content}
    </div>
    """
    return menu


@clientes_bp.route("/clientes", methods=["GET", "POST"])
@login_required
def clientes():
    # INSERÇÃO
    if request.method == "POST":
        codigo = request.form["codigo"]
        nome = request.form["nome"]
        cidade = request.form["cidade"]
        uf = request.form["uf"]
        representante = session.get("codigo")

        inserir_cliente(codigo, nome, cidade, uf, representante)

        return redirect("/clientes")

    # FILTRO
    rep = session.get("codigo")

    if session.get("role") in ["admin", "diretor"]:
        dados = listar_clientes()
    else:
        dados = listar_clientes(rep)

    # HTML
    tabela = ""
    for c in dados:
        tabela += f"<tr><td>{c[1]}</td><td>{c[2]}</td><td>{c[3]}</td><td>{c[4]}</td></tr>"

    content = f"""
    <h1>Clientes</h1>

    <h3>Novo Cliente</h3>
    <form method="post">
        Código: <input name="codigo"><br>
        Nome: <input name="nome"><br>
        Cidade: <input name="cidade"><br>
        UF: <input name="uf"><br>
        <button type="submit">Salvar</button>
    </form>

    <h3>Lista de Clientes</h3>
    <table border="1" cellpadding="5">
        <tr><th>Código</th><th>Nome</th><th>Cidade</th><th>UF</th></tr>
        {tabela}
    </table>
    """

    return render_base(content)

@clientes_bp.route("/clientes-inteligente")
@login_required
def clientes_inteligente():

    rep = session.get("codigo")

    if session.get("role") in ["admin", "diretor"]:
        dados = clientes_com_prioridade()
    else:
        dados = clientes_com_prioridade(rep)

    tabela = ""
    for c in dados:
        tabela += f"""
        <tr>
            <td>{c[1]}</td>
            <td>{c[2]}</td>
            <td>{c[3]}</td>
            <td>{c[4]}</td>
            <td><a href="/atendimentos">Atender</a></td>
        </tr>
        """

    content = f"""
    <h1>🔥 Lista Inteligente de Clientes</h1>

    <table border="1" cellpadding="5">
        <tr>
            <th>Cliente</th>
            <th>Cidade</th>
            <th>UF</th>
            <th>Prioridade</th>
            <th>Ação</th>
        </tr>
        {tabela}
    </table>
    """

    return render_base(content)

    return render_base(content)