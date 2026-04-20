# -*- coding: utf-8 -*-
from html import escape


def render_nova_saf_prorrogar_sem_juros(dados=None, nome_atendente=""):
    dados = dados or {}

    codigo_cliente = dados.get("codigo_cliente", "") or ""
    razao_social = dados.get("razao_social", "") or ""
    supervisor = dados.get("supervisor", "") or ""
    codigo_representante = dados.get("codigo_representante", "") or ""
    representante = dados.get("representante", "") or ""
    ocorrencia_geral = dados.get("ocorrencia_geral", "") or ""
    prioridade = (dados.get("prioridade", "") or "NORMAL").upper()

    prioridade_normal = "selected" if prioridade == "NORMAL" else ""
    prioridade_urgente = "selected" if prioridade == "URGENTE" else ""

    return f"""
    <div class="panel">
        <h3>PRORROGAR SEM JUROS (MOTIVO)</h3>
        <p class="hint">Informe o cliente e adicione os títulos que entrarão na SAF.</p>

        <input type="hidden" name="tipo_saf" value="PRORROGAR_SEM_JUROS">

        <div class="grid-3">
            <div class="field">
                <label>Código do Cliente</label>
                <input type="text" id="codigo_cliente" name="codigo_cliente" value="{escape(str(codigo_cliente))}" placeholder="Digite o código do cliente" onblur="carregarTitulosClienteProrrogacao()">
            </div>

            <div class="field">
                <label>Razão Social</label>
                <input type="text" id="razao_social" name="razao_social" value="{escape(str(razao_social))}" placeholder="Razão social">
            </div>

            <div class="field">
                <label>Atendente</label>
                <input type="text" value="{escape(str(nome_atendente))}" readonly>
            </div>
        </div>

        <div class="grid-3">
            <div class="field">
                <label>Supervisor</label>
                <input type="text" id="supervisor" name="supervisor" value="{escape(str(supervisor))}" placeholder="Supervisor">
            </div>

            <div class="field">
                <label>Código do Representante</label>
                <input type="text" id="codigo_representante" name="codigo_representante" value="{escape(str(codigo_representante))}" placeholder="Código do representante">
            </div>

            <div class="field">
                <label>Representante</label>
                <input type="text" id="representante" name="representante" value="{escape(str(representante))}" placeholder="Representante">
            </div>
        </div>

        <div class="grid-2">
            <div class="field">
                <label>Motivo / Observações Gerais</label>
                <textarea id="observacao" name="ocorrencia_geral" rows="4" placeholder="Descreva o motivo da prorrogação sem juros">{escape(str(ocorrencia_geral))}</textarea>
            </div>

            <div class="field">
                <label>Prioridade</label>
                <select id="prioridade" name="prioridade">
                    <option value="NORMAL" {prioridade_normal}>Normal</option>
                    <option value="URGENTE" {prioridade_urgente}>Urgente</option>
                </select>
            </div>
        </div>

        <div class="panel" style="margin-top:16px;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
                <h4 style="margin:0;">Títulos</h4>
                <input type="text" id="busca_titulo" placeholder="Pesquisar título" oninput="filtrarTitulosClienteProrrogacao()" style="max-width:260px;">
            </div>

            <div id="lista_titulos_cliente" style="margin-top:12px;"></div>
        </div>

        <div id="prorrogar_sem_juros_total_geral" class="panel" style="margin-top:16px;">
            <div style="font-weight:800;font-size:18px;color:#111827;">Total geral da SAF</div>
            <div id="soma_total_titulos" style="margin-top:8px;font-size:26px;font-weight:900;color:#ea580c;">0,00</div>
        </div>

        <div id="lista_titulos_selecionados" style="margin-top:16px;"></div>

        <script>
        (function() {{
            function numeroSeguro(v) {{
                if (v === null || v === undefined || v === '') return 0;
                if (typeof v === 'number') return Number.isFinite(v) ? v : 0;
                const bruto = String(v).trim();
                if (!bruto) return 0;
                const normalizado = bruto.includes(',') ? bruto.replace(/\./g, '').replace(',', '.') : bruto;
                const n = Number(normalizado);
                return Number.isFinite(n) ? n : 0;
            }}

            function atualizarTotalGeralFallback() {{
                if (!Array.isArray(window.titulosSelecionados)) return;
                const soma = window.titulosSelecionados.reduce((acc, item) => acc + numeroSeguro(item.total || item.valor || 0), 0);
                document.querySelectorAll('#soma_total_titulos').forEach(el => {{
                    el.innerText = soma.toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
                }});
            }}

            setInterval(atualizarTotalGeralFallback, 500);
            document.addEventListener('DOMContentLoaded', atualizarTotalGeralFallback);
        }})();
        </script>
    </div>
    """
