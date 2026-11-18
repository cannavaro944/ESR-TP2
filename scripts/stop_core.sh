#!/usr/bin/env bash
# Pára todos os processos do projeto nos contentores da sessão CORE ativa.

set -euo pipefail

SESSION_DIR=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --session) SESSION_DIR="$2"; shift 2 ;;
        *) shift ;;
    esac
done

if [[ -z "$SESSION_DIR" ]]; then
    SESSION_DIR="$(ls -d /tmp/pycore.* 2>/dev/null | head -n 1 || true)"
fi

if [[ -z "$SESSION_DIR" || ! -d "$SESSION_DIR" ]]; then
    echo "[INFO] Nenhuma sessão CORE ativa encontrada em /tmp/pycore.*"
    exit 0
fi

for sock in "$SESSION_DIR"/*; do
    node="$(basename "$sock")"
    # Ignorar ficheiros de controlo internos que não são sockets de nós
    [[ -S "$sock" ]] || continue
    vcmd -c "$sock" -- pkill -f "src\.controlo\.(server|ott)|src\.dados\.video_client" >/dev/null 2>&1 || true
done

echo "[OK] Processos ESR TP2 terminados na sessão $SESSION_DIR."
