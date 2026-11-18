#!/usr/bin/env bash
# Automação de arranque da rede Overlay no emulador CORE (executar no Ubuntu host).
#
# Modos:
#   ./scripts/launch_core.sh          -> Abre separadores interativos limpos (gnome-terminal) por nó
#   ./scripts/launch_core.sh --auto   -> Arranca tudo em background (--daemon), inicia stream em S1
#                                        e abre automaticamente VideoClient em n20 e n21

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

AUTO_MODE=0
SESSION_DIR=""
BOOT_NODE="S1"
BOOT_IP="10.0.19.21"
TCP_PORT="9001"
STREAM_ID="Stream1"
VIDEO_FILE="assets/movie.Mjpeg"
FPS="20"
SERVER_NODE="S1"
NODES="S1 n10 n9 n5 n20 n21"
CLIENTS="n20 n21"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --auto)      AUTO_MODE=1; shift ;;
        --session)   SESSION_DIR="$2"; shift 2 ;;
        --boot-ip)   BOOT_IP="$2"; shift 2 ;;
        --boot-node) BOOT_NODE="$2"; shift 2 ;;
        --port)      TCP_PORT="$2"; shift 2 ;;
        --video)     VIDEO_FILE="$2"; shift 2 ;;
        --stream)    STREAM_ID="$2"; shift 2 ;;
        --fps)       FPS="$2"; shift 2 ;;
        --nodes)     NODES="$2"; shift 2 ;;
        --clients)   CLIENTS="$2"; shift 2 ;;
        -h|--help)
            echo "Uso: $0 [--auto] [--session /tmp/pycore.X] [--boot-ip 10.0.19.21] [--video assets/movie.Mjpeg]"
            exit 0
            ;;
        *)
            echo "Opção desconhecida: $1"
            exit 1
            ;;
    esac
done

if [[ -z "$SESSION_DIR" ]]; then
    SESSION_DIR="$(ls -d /tmp/pycore.* 2>/dev/null | head -n 1 || true)"
fi

if [[ -z "$SESSION_DIR" || ! -d "$SESSION_DIR" ]]; then
    echo "[ERRO] Nenhuma sessão CORE ativa encontrada em /tmp/pycore.*"
    echo "       Abra o cenário topologias/topology_tp2.xml no CORE e carregue em Start primeiro."
    exit 1
fi

if ! command -v vcmd >/dev/null 2>&1; then
    echo "[ERRO] Comando 'vcmd' do CORE não encontrado no PATH."
    exit 1
fi

echo "[*] Sessão CORE detetada: $SESSION_DIR"

# Permitir janelas Tkinter vindas dos contentores Linux (root) no ecrã local
if command -v xhost >/dev/null 2>&1; then
    xhost +local: >/dev/null 2>&1 || true
fi

export DISPLAY="${DISPLAY:-:0}"

# Limpar processos anteriores para evitar portas presas
"$SCRIPT_DIR/stop_core.sh" --session "$SESSION_DIR" >/dev/null 2>&1 || true

if [[ "$AUTO_MODE" -eq 1 ]]; then
    echo "[*] Modo AUTOMÁTICO (--auto): a arrancar todos os nós em background..."

    # 1. Bootstrapper
    vcmd -c "$SESSION_DIR/$BOOT_NODE" -- bash -c \
        "cd '$PROJECT_DIR' && python3 -u -m src.controlo.server > /tmp/esr_bootstrapper.log 2>&1 &"
    sleep 1

    # 2. Nós intermédios primeiro (excluindo servidor e clientes)
    for node in $NODES; do
        if [[ "$node" == "$SERVER_NODE" ]] || [[ " $CLIENTS " == *" $node "* ]]; then
            continue
        fi
        echo "    -> A iniciar nó intermédio $node..."
        vcmd -c "$SESSION_DIR/$node" -- bash -c \
            "cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -u -m src.controlo.ott '$node' '$BOOT_IP' '$TCP_PORT' --daemon > '/tmp/esr_${node}.log' 2>&1 &"
    done
    sleep 1.5

    # 3. Nó servidor (com stream automático)
    echo "    -> A iniciar servidor $SERVER_NODE com stream '$STREAM_ID' ($VIDEO_FILE @ ${FPS}fps)..."
    vcmd -c "$SESSION_DIR/$SERVER_NODE" -- bash -c \
        "cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -u -m src.controlo.ott '$SERVER_NODE' '$BOOT_IP' '$TCP_PORT' --daemon --stream '$STREAM_ID' '$VIDEO_FILE' '$FPS' > '/tmp/esr_${SERVER_NODE}.log' 2>&1 &"
    sleep 2.0

    # 4. Nós clientes (com --play automático)
    for client in $CLIENTS; do
        echo "    -> A iniciar cliente $client (--play $STREAM_ID)..."
        vcmd -c "$SESSION_DIR/$client" -- bash -c \
            "cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -u -m src.controlo.ott '$client' '$BOOT_IP' '$TCP_PORT' --daemon --play '$STREAM_ID' > '/tmp/esr_${client}.log' 2>&1 &"
    done

    echo ""
    echo "[OK] Overlay arrancado em modo automático!"
    echo "     Logs disponíveis em /tmp/esr_<node>.log (ex: tail -f /tmp/esr_S1.log)"
    echo "     Para parar tudo: ./scripts/stop_core.sh"
    exit 0
fi

# Modo interativo em separadores limpos (evita xterms bugados do CORE)
if command -v gnome-terminal >/dev/null 2>&1; then
    echo "[*] A abrir janela gnome-terminal com separadores para cada nó..."
    TABS=()
    TABS+=(--tab --title="Boot ($BOOT_NODE)" -- bash -c "vcmd -c '$SESSION_DIR/$BOOT_NODE' -- bash -c \"cd '$PROJECT_DIR' && python3 -m src.controlo.server\"; exec bash")

    for node in $NODES; do
        if [[ "$node" == "$SERVER_NODE" ]]; then
            TABS+=(--tab --title="$node (Srv)" -- bash -c "sleep 1; vcmd -c '$SESSION_DIR/$node' -- bash -c \"cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -m src.controlo.ott '$node' '$BOOT_IP' '$TCP_PORT' --stream '$STREAM_ID' '$VIDEO_FILE' '$FPS'\"; exec bash")
        else
            TABS+=(--tab --title="$node" -- bash -c "sleep 1.5; vcmd -c '$SESSION_DIR/$node' -- bash -c \"cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -m src.controlo.ott '$node' '$BOOT_IP' '$TCP_PORT'\"; exec bash")
        fi
    done

    gnome-terminal "${TABS[@]}" &
    echo "[OK] Separadores abertos! Escreva 'play $STREAM_ID' nos separadores dos clientes (n20, n21)."
else
    echo "[AVISO] 'gnome-terminal' não encontrado. Use './scripts/launch_core.sh --auto'"
    echo "        ou abra cada nó e corra './scripts/start_node.sh <node_id>'."
    exit 1
fi
