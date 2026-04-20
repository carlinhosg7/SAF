# -*- coding: utf-8 -*-
from html import escape

def render_nova_saf_prorrogar_com_juros(dados=None, nome_atendente=""):
    dados = dados or {}
    codigo_cliente = escape(str(dados.get("codigo_cliente") or ""))
    razao_social = escape(str(dados.get("razao_social") or ""))
    supervisor = escape(str(dados.get("supervisor") or ""))
    codigo_representante = escape(str(dados.get("codigo_representante") or ""))
    representante = escape(str(dados.get("representante") or ""))
    ocorrencia_geral = escape(str(dados.get("ocorrencia_geral") or ""))
    prioridade = escape(str(dados.get("prioridade") or "NORMAL"))
    nome_atendente = escape(str(nome_atendente or ""))

    return f"""
    <div class="panel">
        <h3>PRORROGAR COM JUROS (MOTIVO)</h3>
        <div class="grid-2">
            <div class="field">
                <label>Código Cliente</label>
                <input type="text" id="codigo_cliente" value="{codigo_cliente}" autocomplete="off">
            </div>
            <div class="field">
                <label>Razão Social</label>
                <input type="text" id="razao_social" value="{razao_social}" readonly>
            </div>
            <div class="field">
                <label>Código Representante</label>
                <input type="text" id="codigo_representante" value="{codigo_representante}" readonly>
            </div>
            <div class="field">
                <label>Representante</label>
                <input type="text" id="representante" value="{representante}" readonly>
            </div>
            <div class="field">
                <label>Supervisor</label>
                <input type="text" id="supervisor" value="{supervisor}" readonly>
            </div>
            <div class="field">
                <label>Atendente</label>
                <input type="text" value="{nome_atendente}" readonly>
            </div>
        </div>

        <div class="grid-2">
            <div class="field">
                <label>Prioridade</label>
                <select id="prioridade">
                    <option value="NORMAL" {"selected" if prioridade == "NORMAL" else ""}>Normal</option>
                    <option value="URGENTE" {"selected" if prioridade == "URGENTE" else ""}>Urgente</option>
                </select>
            </div>
            <div class="field">
                <label>Observações</label>
                <textarea id="observacao" rows="3" placeholder="Motivo da prorrogação com juros">{ocorrencia_geral}</textarea>
            </div>
        </div>
    </div>
    """
