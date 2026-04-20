from flask import Blueprint, request, redirect, session
from utils.decorators import login_required
from services.atendimento_service import inserir_atendimento, listar_atendimentos, listar_clientes_select

atendimentos_bp = Blueprint("atendimentos", __name__)

def render_base(content):
    menu = f"""
    <div style='width:220px;float:left;background:#111;color:#fff;height:100vh;padding:15px'>
        <h3>SAF</h3>
        <p>{session.get("nome")}</p>
        <hr>
        <a href="/dashboard" style="color:white">Dashboard</a><br>
        <a href="/clientes" style="color:white">Clientes</a><br>
        <a href="/atendimentos" style="color:white">Atendimentos</a><br>
        <a href="/relatorios" style="color:white">Relatórios</a><br>
        <a href="/logout" style="color:red">Sair</a>
    </div>
    <div style='margin-left:240px;padding:20px'>
        {content}
    </div>
    """
    return menu


@atendimentos_bp.route("/atendimentos", methods=["GET", "POST"])
@login_required
def atendimentos():

    rep = session.get("codigo")

    # SALVAR
    if request.method == "POST":
        cliente_id = request.form["cliente_id"]
        status = request.form["status"]
        observacao = request.form["observacao"]

        inserir_atendimento(cliente_id, rep, status, observacao)
        return redirect("/atendimentos")

    # LISTAS
    if session.get("role") in ["admin", "diretor"]:
        atends = listar_atendimentos()
        clientes = listar_clientes_select()
    else:
        atends = listar_atendimentos(rep)
        clientes = listar_clientes_select(rep)

    # SELECT CLIENTES
    options = ""
    for c in clientes:
        options += f"<option value='{c[0]}'>{c[1]}</option>"

    # TABELA
    tabela = ""
    for a in atends:
        tabela += f"<tr><td>{a[1]}</td><td>{a[2]}</td><td>{a[3]}</td></tr>"

    content = f"""
    <h1>Atendimentos</h1>

    <h3>Novo Atendimento</h3>
    <form method="post">
        Cliente:
        <select name="cliente_id">{options}</select><br>

        Status:
        <select name="status">
            <option>Visitado</option>
            <option>Não atendeu</option>
            <option>Pedido realizado</option>
            <option>Retornar depois</option>
        </select><br>

        Observação:<br>
        <textarea name="observacao"></textarea><br>

        <button type="submit">Salvar</button>
    </form>

    <h3>Histórico</h3>
    <table border="1" cellpadding="5">
        <tr><th>Cliente</th><th>Data</th><th>Status</th></tr>
        {tabela}
    </table>
    """

    return render_base(content)