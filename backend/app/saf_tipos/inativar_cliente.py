# -*- coding: utf-8 -*-
import json
from datetime import datetime
from html import escape

def render_nova_saf_inativar_cliente(*, representante_info: dict, nome_atendente: str) -> str:
    representante_info = representante_info or {}
    nome_atendente = nome_atendente or ""
    data_hoje = datetime.now().strftime('%d/%m/%Y')

    return f"""
        <div class="panel">
            <h3>Dados base da solicitação</h3>
            <div class="grid-2">
                <div class="field">
                    <label>Data</label>
                    <input type="text" value="{data_hoje}" readonly>
                </div>
                <div class="field">
                    <label>Atendente</label>
                    <input type="text" value="{escape(nome_atendente)}" readonly>
                </div>
            </div>
            <div class="grid-3">
                <div class="field">
                    <label>Supervisor</label>
                    <input type="text" id="supervisor_base" value="{escape(representante_info.get('supervisor') or '')}" readonly>
                </div>
                <div class="field">
                    <label>Código Representante</label>
                    <input type="text" id="codigo_representante_base" value="{escape(representante_info.get('codigo_representante') or '')}" readonly>
                </div>
                <div class="field">
                    <label>Representante</label>
                    <input type="text" id="representante_base" value="{escape(representante_info.get('representante') or '')}" readonly>
                </div>
            </div>
            <div class="grid-2">
                <div class="field">
                    <label>Tipo de SAF</label>
                    <input type="text" value="Inativar Cliente" readonly>
                    <input type="hidden" id="tipo_saf" value="INATIVAR_CLIENTE">
                </div>
                <div class="field">
                    <label>Prioridade</label>
                    <select id="prioridade_saf">
                        <option value="NORMAL" selected>Normal</option>
                        <option value="URGENTE">Urgente</option>
                    </select>
                </div>
            </div>
        </div>

        <div id="formulario-saf-dinamico"></div>

        <style>
            .picker-wrap {{position:relative; min-width:260px;}}
            .picker-panel {{display:none; position:fixed; top:50%; left:50%; transform:translate(-50%, -50%); width:min(980px, calc(100vw - 24px)); max-height:min(82vh, 760px); background:linear-gradient(180deg,#fff 0%,#fffaf6 100%); border:1px solid #f0c49a; border-radius:22px; box-shadow:0 28px 80px rgba(0,0,0,.24); z-index:10030; padding:18px;}}
            .picker-panel.open {{display:block;}}
            .picker-backdrop {{display:none; position:fixed; inset:0; background:rgba(15,23,42,.28); z-index:10020; backdrop-filter:blur(2px);}}
            .picker-backdrop.open {{display:block;}}
            .picker-head {{display:flex; justify-content:space-between; align-items:center; gap:10px; margin-bottom:14px;}}
            .picker-title {{font-size:18px; font-weight:800; color:#1f2937;}}
            .picker-list {{max-height:calc(min(82vh,760px) - 230px); overflow:auto; border:1px solid #f0d8c2; border-radius:16px; background:#fff; padding:6px;}}
            .picker-item {{display:flex; align-items:flex-start; gap:12px; padding:12px 14px; border-bottom:1px solid #f6ede6; cursor:pointer; border-radius:12px;}}
            .picker-item:last-child {{border-bottom:none;}}
            .picker-item:hover {{background:#fff6ee;}}
            .picker-item input {{margin-top:3px; width:18px; height:18px; accent-color:#f97316;}}
            .picker-item .main {{font-weight:700; color:#111827;}}
            .picker-item .sub {{font-size:13px; color:#6b7280; margin-top:4px;}}
            .picker-toolbar {{display:flex; gap:10px; flex-wrap:wrap; margin-bottom:12px;}}
            .picker-toolbar .btn {{padding:10px 14px; border-radius:12px;}}
            .picker-search input {{width:100%; height:44px; border-radius:12px; padding:0 14px; border:1px solid #e9c8a7; background:#fff; font-size:14px;}}
            .saf-meta-box.cols-6 {{grid-template-columns:repeat(6, minmax(0,1fr));}}
            @media (max-width: 900px) {{ .saf-meta-box.cols-6 {{grid-template-columns:repeat(2,minmax(0,1fr));}} }}
        </style>
        <div id="cliente-picker-backdrop" class="picker-backdrop" onclick="fecharPickerClientes()"></div>

        <script>
            const CODIGO_REP_BASE = {json.dumps(representante_info.get('codigo_representante') or '')};
            let clientesRepCache = [];

            function obterBaseSaf() {{
                return {{
                    codigoCliente: '',
                    cnpj: '',
                    codigoGrupoCliente: '',
                    grupoCliente: '',
                    razaoSocial: '',
                    codigoRepresentante: document.getElementById('codigo_representante_base')?.value || '',
                    representante: document.getElementById('representante_base')?.value || '',
                    endereco: '',
                    enderecoNum: '',
                    bairro: '',
                    cidade: '',
                    uf: '',
                    telefone: '',
                    whatsapp: '',
                    qtdVenda: '',
                    supervisor: document.getElementById('supervisor_base')?.value || '',
                    atendente: {json.dumps(nome_atendente)},
                    dataHoje: {json.dumps(data_hoje)}
                }};
            }}

            function esc(v) {{
                return (v || '').toString().replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
            }}

            function cabecalhoSafHTML(base, titulo, subtitulo) {{
                return `
                    <div class="saf-meta-box cols-6">
                        <div class="saf-meta-item"><span>ID</span><strong>Será gerado ao salvar</strong></div>
                        <div class="saf-meta-item"><span>Data</span><strong>${{esc(base.dataHoje)}}</strong></div>
                        <div class="saf-meta-item"><span>Supervisor</span><strong>${{esc(base.supervisor)}}</strong></div>
                        <div class="saf-meta-item"><span>Atendente</span><strong>${{esc(base.atendente)}}</strong></div>
                        <div class="saf-meta-item"><span>Cód. Representante</span><strong>${{esc(base.codigoRepresentante)}}</strong></div>
                        <div class="saf-meta-item"><span>Representante</span><strong>${{esc(base.representante)}}</strong></div>
                    </div>
                    <div class="table-toolbar"><div><h3 style="margin-bottom:6px;">${{titulo}}</h3><div class="hint">${{subtitulo}}</div></div></div>
                `;
            }}

            function linhaInativarCliente(item = {{}}) {{
                return `
                    <tr>
                        <td class="row-num"></td>
                        <td><input type="text" name="codigo_cliente_item[]" value="${{esc(item.codigo_cliente_item || item.codigo_cliente || '')}}" readonly></td>
                        <td><input type="text" name="razao_social_item[]" value="${{esc(item.razao_social_item || item.razao_social || '')}}" readonly></td>
                        <td><input type="text" name="cnpj_item[]" value="${{esc(item.cnpj_item || item.cnpj || '')}}" readonly></td>
                        <td><input type="text" name="codigo_grupo_cliente_item[]" value="${{esc(item.codigo_grupo_cliente_item || item.codigo_grupo_cliente || '')}}" readonly></td>
                        <td><input type="text" name="grupo_cliente_item[]" value="${{esc(item.grupo_cliente_item || item.grupo_cliente || '')}}" readonly></td>
                        <td style="min-width:220px;">
                            <select name="situacao[]" style="width:220px;min-width:220px;padding:6px;">
                                <option value="">Selecione</option>
                                <option value="ATIVO" ${{item.situacao === 'ATIVO' ? 'selected' : ''}}>Ativo</option>
                                <option value="INATIVO" ${{item.situacao === 'INATIVO' ? 'selected' : ''}}>Inativo</option>
                                <option value="BLOQUEIO_FINANCEIRO" ${{item.situacao === 'BLOQUEIO_FINANCEIRO' ? 'selected' : ''}}>Bloqueio Financeiro</option>
                                <option value="SEM_LIMITE_CREDITO" ${{item.situacao === 'SEM_LIMITE_CREDITO' ? 'selected' : ''}}>Sem Limite de Crédito</option>
                            </select>
                        </td>
                        <td><input type="text" name="acao[]" value="${{esc(item.acao || '')}}" placeholder="Ação"></td>
                        <td><textarea name="ocorrencia_item[]" placeholder="Observações">${{esc(item.ocorrencia_item || '')}}</textarea></td>
                        <td class="col-acoes"><button class="btn btn-danger" type="button" onclick="removerLinha(this)">Remover</button></td>
                    </tr>
                `;
            }}

            function adicionarLinhaClienteItem(item) {{
                const tbody = document.getElementById('tbody_inativar_cliente');
                if (!tbody || !item) return;
                const existente = Array.from(tbody.querySelectorAll('input[name="codigo_cliente_item[]"]')).some(el => (el.value || '').trim() === (item.codigo_cliente || '').trim());
                if (existente) return;
                tbody.insertAdjacentHTML('beforeend', linhaInativarCliente(item));
                renumerarLinhas();
            }}

            function renumerarLinhas() {{
                document.querySelectorAll('#tbody_inativar_cliente tr').forEach((tr, idx) => {{
                    const cell = tr.querySelector('.row-num');
                    if (cell) cell.textContent = String(idx + 1);
                }});
            }}

            function removerLinha(btn) {{
                const tr = btn?.closest('tr');
                if (tr) tr.remove();
                renumerarLinhas();
            }}

            function coletarItensTabela(tbodyId) {{
                const tbody = document.getElementById(tbodyId);
                if (!tbody) return [];
                return Array.from(tbody.querySelectorAll('tr')).map(tr => {{
                    const get = (sel) => tr.querySelector(sel)?.value || '';
                    return {{
                        codigo_cliente_item: get('input[name="codigo_cliente_item[]"]'),
                        razao_social_item: get('input[name="razao_social_item[]"]'),
                        cnpj_item: get('input[name="cnpj_item[]"]'),
                        codigo_grupo_cliente_item: get('input[name="codigo_grupo_cliente_item[]"]'),
                        grupo_cliente_item: get('input[name="grupo_cliente_item[]"]'),
                        situacao: get('select[name="situacao[]"]'),
                        acao: get('input[name="acao[]"]'),
                        ocorrencia_item: get('textarea[name="ocorrencia_item[]"]')
                    }};
                }});
            }}

            async function carregarClientesRepresentante(termo = '') {{
                const box = document.getElementById('cliente_picker_list');
                if (!box) return;
                box.innerHTML = '<div class="hint" style="padding:12px;">Carregando clientes...</div>';
                try {{
                    const resp = await fetch(`/api/inativacao-clientes-busca?codigo_representante=${{encodeURIComponent(CODIGO_REP_BASE)}}&q=${{encodeURIComponent(termo)}}`);
                    const data = await resp.json();
                    if (!resp.ok || !data.ok) throw new Error(data.erro || 'Erro ao carregar clientes.');
                    clientesRepCache = Array.isArray(data.items) ? data.items : [];
                    renderClientesPicker();
                }} catch (e) {{
                    box.innerHTML = `<div class="hint" style="padding:12px;color:#b91c1c;">${{esc(e.message || e)}}</div>`;
                }}
            }}

            function renderClientesPicker() {{
                const box = document.getElementById('cliente_picker_list');
                if (!box) return;
                if (!clientesRepCache.length) {{
                    box.innerHTML = '<div class="hint" style="padding:12px;">Nenhum cliente/grupo encontrado para este representante.</div>';
                    atualizarContadorPicker();
                    return;
                }}
                box.innerHTML = clientesRepCache.map((item, idx) => `
                    <label class="picker-item">
                        <input type="checkbox" class="cliente-picker-check" data-index="${{idx}}">
                        <div>
                            <div class="main">${{esc(item.codigo_cliente || '')}} - ${{esc(item.razao_social || '')}}</div>
                            <div class="sub">Grupo: ${{esc(item.codigo_grupo_cliente || '')}} - ${{esc(item.grupo_cliente || '')}} | CNPJ: ${{esc(item.cnpj || '')}}</div>
                        </div>
                    </label>
                `).join('');
                atualizarContadorPicker();
                box.querySelectorAll('.cliente-picker-check').forEach(el => el.addEventListener('change', atualizarContadorPicker));
            }}

            function atualizarContadorPicker() {{
                const total = document.querySelectorAll('.cliente-picker-check:checked').length;
                const counter = document.getElementById('cliente_picker_counter');
                if (counter) counter.textContent = total ? `${{total}} item(ns) selecionado(s)` : 'Nenhum item selecionado';
            }}

            function marcarTodosClientesPicker(marcar) {{
                document.querySelectorAll('.cliente-picker-check').forEach(el => el.checked = !!marcar);
                atualizarContadorPicker();
            }}

            function abrirPickerClientes() {{
                const panel = document.getElementById('cliente-picker-panel');
                const backdrop = document.getElementById('cliente-picker-backdrop');
                if (panel) panel.classList.add('open');
                if (backdrop) backdrop.classList.add('open');
                carregarClientesRepresentante(document.getElementById('cliente_picker_q')?.value || '');
            }}

            function fecharPickerClientes() {{
                const panel = document.getElementById('cliente-picker-panel');
                const backdrop = document.getElementById('cliente-picker-backdrop');
                if (panel) panel.classList.remove('open');
                if (backdrop) backdrop.classList.remove('open');
            }}

            function aplicarSelecaoClientesPicker() {{
                const selecionados = Array.from(document.querySelectorAll('.cliente-picker-check:checked')).map(el => clientesRepCache[parseInt(el.dataset.index || '-1', 10)]).filter(Boolean);
                selecionados.forEach(item => adicionarLinhaClienteItem(item));
                fecharPickerClientes();
            }}

            function tabelaPadrao(idTabela) {{
                return `
                    <div class="table-toolbar">
                        <div class="hint">Selecione múltiplos clientes/grupos do representante e complete Situação, Ação e Observações.</div>
                        <div style="display:flex; gap:10px; flex-wrap:wrap;">
                            <button class="btn btn-outline" type="button" onclick="abrirPickerClientes()">Selecionar clientes / grupos</button>
                        </div>
                    </div>
                    <div class="saf-table-wrap">
                        <table class="saf-table">
                            <thead>
                                <tr>
                                    <th>#</th>
                                    <th>Código Cliente</th>
                                    <th>Razão Social</th>
                                    <th>CNPJ</th>
                                    <th>Código Grupo Cliente</th>
                                    <th>Grupo Cliente</th>
                                    <th>Situação</th>
                                    <th>Ação</th>
                                    <th>Observações</th>
                                    <th class="col-acoes">Ações</th>
                                </tr>
                            </thead>
                            <tbody id="${{idTabela}}"></tbody>
                        </table>
                    </div>
                    <div class="field">
                        <label>Observações gerais</label>
                        <textarea id="ocorrencia_geral_saf" data-role="ocorrencia-geral" placeholder="Observações gerais da solicitação"></textarea>
                    </div>
                    <div class="field">
                        <label>Anexos da SAF</label>
                        <input type="file" id="anexos_nova_saf" multiple style="display:none" onchange="atualizarResumoAnexosNovaSaf()">
                        <div style="display:flex; gap:12px; flex-wrap:wrap; align-items:center;">
                            <button class="btn btn-outline" type="button" onclick="abrirSeletorAnexosNovaSaf()">Anexar arquivo(s)</button>
                            <span id="anexos_nova_saf_resumo" class="hint">Nenhum arquivo selecionado.</span>
                        </div>
                    </div>
                    <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:12px;">
                        <button class="btn" type="button" onclick="salvarSaf('INATIVAR_CLIENTE', '${{idTabela}}')">Salvar SAF</button>
                        <button class="btn btn-outline" type="button" onclick="mostrarFormularioSaf()">Recarregar formulário</button>
                    </div>
                `;
            }}

            function abrirSeletorAnexosNovaSaf() {{ const input = document.getElementById('anexos_nova_saf'); if (input) input.click(); }}
            function atualizarResumoAnexosNovaSaf() {{
                const input = document.getElementById('anexos_nova_saf');
                const resumo = document.getElementById('anexos_nova_saf_resumo');
                if (!resumo) return;
                if (!input || !input.files || !input.files.length) {{ resumo.textContent = 'Nenhum arquivo selecionado.'; return; }}
                resumo.textContent = input.files.length === 1 ? input.files[0].name : `${{input.files.length}} arquivo(s) selecionado(s)`;
            }}
            async function enviarAnexosNovaSaf(safId) {{
                const input = document.getElementById('anexos_nova_saf');
                if (!input || !input.files || !input.files.length) return {{ ok: true, arquivos: [] }};
                const formData = new FormData();
                for (const arquivo of input.files) formData.append('arquivos', arquivo);
                const resp = await fetch(`/saf/${{safId}}/anexos`, {{ method:'POST', body: formData }});
                const data = await resp.json();
                if (!resp.ok || !data.ok) throw new Error(data.erro || 'Erro ao enviar anexos da SAF.');
                return data;
            }}
            async function salvarSaf(tipo, tbodyId) {{
                const base = obterBaseSaf();
                const itens = coletarItensTabela(tbodyId).filter(item => (item.codigo_cliente_item || item.razao_social_item || item.codigo_grupo_cliente_item || item.grupo_cliente_item));
                const ocorrenciaGeral = document.getElementById('ocorrencia_geral_saf')?.value || '';
                if (!itens.length) {{ alert('Selecione ao menos um cliente/grupo para a inativação.'); return; }}
                const payload = {{
                    tipo_saf: tipo,
                    supervisor: base.supervisor,
                    codigo_representante: base.codigoRepresentante,
                    representante: base.representante,
                    codigo_cliente: '',
                    cnpj: '',
                    codigo_grupo_cliente: '',
                    grupo_cliente: '',
                    razao_social: '',
                    endereco: '',
                    endereco_num: '',
                    bairro: '',
                    cidade: '',
                    uf: '',
                    telefone: '',
                    whatsapp: '',
                    qtd_venda: '',
                    ocorrencia_geral: ocorrenciaGeral,
                    prioridade: (document.getElementById('prioridade_saf')?.value || 'NORMAL'),
                    itens: itens
                }};
                try {{
                    const resp = await fetch('/salvar-saf', {{ method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify(payload) }});
                    const data = await resp.json();
                    if (!resp.ok || !data.ok) {{ alert(data.erro || 'Erro ao salvar SAF.'); return; }}
                    let mensagem = `SAF salva com sucesso! ID: ${{data.saf_id}}`;
                    try {{
                        const anexosData = await enviarAnexosNovaSaf(data.saf_id);
                        if (anexosData && anexosData.arquivos && anexosData.arquivos.length) mensagem += `\n${{anexosData.arquivos.length}} anexo(s) enviado(s) com sucesso.`;
                    }} catch (erroAnexo) {{ mensagem += `\nErro ao enviar anexos: ${{erroAnexo.message || erroAnexo}}`; }}
                    alert(mensagem);
                    window.location.href = `/saf/${{data.saf_id}}`;
                }} catch (e) {{ alert('Erro ao salvar SAF: ' + e); }}
            }}

            function montarFormularioInativar(base) {{
                return `
                    <div class="panel">
                        ${{cabecalhoSafHTML(base, 'Formulário - Inativar Cliente', 'Primeiro o representante. Depois seleção múltipla de cliente/grupo para montar a grade da inativação.') }}
                        ${{tabelaPadrao('tbody_inativar_cliente')}}
                    </div>
                    <div id="cliente-picker-panel" class="picker-panel">
                        <div class="picker-head">
                            <div><div class="picker-title">Selecionar clientes / grupos</div><div class="hint">Busca por código ou nome do cliente e do grupo.</div></div>
                            <button class="btn btn-outline" type="button" onclick="fecharPickerClientes()">Fechar</button>
                        </div>
                        <div class="picker-toolbar">
                            <button class="btn btn-outline" type="button" onclick="marcarTodosClientesPicker(true)">Marcar todos</button>
                            <button class="btn btn-outline" type="button" onclick="marcarTodosClientesPicker(false)">Limpar</button>
                            <button class="btn" type="button" onclick="aplicarSelecaoClientesPicker()">Inserir selecionados</button>
                        </div>
                        <div class="picker-search" style="margin-bottom:12px;">
                            <input id="cliente_picker_q" type="text" placeholder="Buscar por código/nome do cliente ou grupo" oninput="carregarClientesRepresentante(this.value)">
                        </div>
                        <div id="cliente_picker_list" class="picker-list"></div>
                        <div class="picker-head" style="margin-top:12px; margin-bottom:0;"><div id="cliente_picker_counter" class="hint">Nenhum item selecionado</div></div>
                    </div>
                `;
            }}

            function mostrarFormularioSaf() {{
                const area = document.getElementById('formulario-saf-dinamico');
                if (!area) return;
                area.innerHTML = montarFormularioInativar(obterBaseSaf());
            }}
            window.addEventListener('DOMContentLoaded', function() {{ if (document.getElementById('formulario-saf-dinamico')) mostrarFormularioSaf(); }});
        </script>
        """
