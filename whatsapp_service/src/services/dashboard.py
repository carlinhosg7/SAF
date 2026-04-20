from flask import Blueprint, session
from utils.decorators import login_required
from services.dashboard_service import get_kpis, ultimos_atendimentos

dashboard_bp = Blueprint("dashboard", __name__)

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


@dashboard_bp.route("/dashboard")
@login_required
def dashboard():

    rep = session.get("codigo")

    if session.get("role") in ["admin", "diretor"]:
        kpis = get_kpis()
        ultimos = ultimos_atendimentos()
    else:
        kpis = get_kpis(rep)
        ultimos = ultimos_atendimentos(rep)

    # TABELA
    tabela = ""
    for u in ultimos:
        tabela += f"<tr><td>{u[0]}</td><td>{u[1]}</td><td>{u[2]}</td></tr>"

    content = f"""
    <h1>Dashboard Inteligente</h1>

    <div style="display:flex;gap:20px;margin-bottom:30px">
        <div style="background:#222;color:white;padding:20px">Clientes: {kpis['clientes']}</div>
        <div style="background:#222;color:white;padding:20px">Atendimentos Hoje: {kpis['atendimentos_hoje']}</div>
        <div style="background:#222;color:white;padding:20px">Sem Atendimento: {kpis['sem_atendimento']}</div>
        <div style="background:#222;color:white;padding:20px">Conversão: {kpis['conversao']}%</div>
    </div>

    <h3>Últimos Atendimentos</h3>
    <table border="1" cellpadding="5">
        <tr><th>Cliente</th><th>Data</th><th>Status</th></tr>
        {tabela}
    </table>
    """

    return render_base(content)