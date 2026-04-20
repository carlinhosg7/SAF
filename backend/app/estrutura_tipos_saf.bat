@echo off
setlocal

REM Estrutura base para separar tipos de SAF em arquivos independentes
set BASE_DIR=%~dp0saf_tipos

if not exist "%BASE_DIR%" mkdir "%BASE_DIR%"
if not exist "%BASE_DIR%\__init__.py" type nul > "%BASE_DIR%\__init__.py"
if not exist "%BASE_DIR%\inativar_cliente.py" type nul > "%BASE_DIR%\inativar_cliente.py"
if not exist "%BASE_DIR%\alterar_portador_devolucao.py" type nul > "%BASE_DIR%\alterar_portador_devolucao.py"
if not exist "%BASE_DIR%\alterar_portador_diversos.py" type nul > "%BASE_DIR%\alterar_portador_diversos.py"
if not exist "%BASE_DIR%\prorrogar_sem_juros.py" type nul > "%BASE_DIR%\prorrogar_sem_juros.py"
if not exist "%BASE_DIR%\prorrogar_com_juros.py" type nul > "%BASE_DIR%\prorrogar_com_juros.py"
if not exist "%BASE_DIR%\negociacao_titulos_reparcelamento.py" type nul > "%BASE_DIR%\negociacao_titulos_reparcelamento.py"
if not exist "%BASE_DIR%\baixar_credito_cliente.py" type nul > "%BASE_DIR%\baixar_credito_cliente.py"
if not exist "%BASE_DIR%\creditar_cliente.py" type nul > "%BASE_DIR%\creditar_cliente.py"
if not exist "%BASE_DIR%\carta_anuencia.py" type nul > "%BASE_DIR%\carta_anuencia.py"
if not exist "%BASE_DIR%\descontos_diversos.py" type nul > "%BASE_DIR%\descontos_diversos.py"
if not exist "%BASE_DIR%\comuns.py" type nul > "%BASE_DIR%\comuns.py"

echo Estrutura criada em: %BASE_DIR%
pause
