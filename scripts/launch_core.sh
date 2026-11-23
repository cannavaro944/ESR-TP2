#!/usr/bin/env bash
# Automação de arranque da rede Overlay no emulador CORE (executar no Ubuntu host).
# Por defeito NÃO inicia nenhum fluxo nem cliente de vídeo: apenas prepara o
# Bootstrapper e os nós do overlay para que os comandos (stream / play) sejam
# executados manualmente.
#
# Modos:
#   ./scripts/launch_core.sh                     -> Abre terminais interativos (gnome-terminal ou xterm)
#                                                   para o Bootstrapper e todos os nós, sem iniciar fluxos
#   ./scripts/launch_core.sh --auto              -> Arranca Bootstrapper + nós intermédios em background
#                                                   (--daemon), sem iniciar fluxos (deixa S1 e clientes
#                                                   livres para lançar manualmente no CORE)
#   ./scripts/launch_core.sh --auto --with-stream -> Arranca tudo em background incluindo stream em S1
#                                                    e VideoClient nos clientes

set -euo pipefail

ORIG_ARGS=("$@")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

AUTO_MODE=0
WITH_STREAM=0
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
        --auto|--bg)    AUTO_MODE=1; shift ;;
        --with-stream)  WITH_STREAM=1; shift ;;
        --session)      SESSION_DIR="$2"; shift 2 ;;
        --boot-ip)      BOOT_IP="$2"; shift 2 ;;
        --boot-node)    BOOT_NODE="$2"; shift 2 ;;
        --port)         TCP_PORT="$2"; shift 2 ;;
        --video)        VIDEO_FILE="$2"; shift 2 ;;
        --stream)       STREAM_ID="$2"; shift 2 ;;
        --fps)          FPS="$2"; shift 2 ;;
        --nodes)        NODES="$2"; shift 2 ;;
        --clients)      CLIENTS="$2"; shift 2 ;;
        -h|--help)
            echo "Uso: $0 [--auto|--bg] [--with-stream] [--session /tmp/pycore.X] [--boot-ip 10.0.19.21]"
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

if [[ -S "$SESSION_DIR/$BOOT_NODE" && ! -w "$SESSION_DIR/$BOOT_NODE" ]]; then
    echo "[ERRO] Sem permissão para aceder ao socket $SESSION_DIR/$BOOT_NODE."
    echo "       Execute com: sudo -E $0 ${ORIG_ARGS[*]}"
    exit 1
fi

# Permitir janelas Tkinter vindas dos contentores Linux (root) no ecrã local
if command -v xhost >/dev/null 2>&1; then
    xhost +local: >/dev/null 2>&1 || true
fi

export DISPLAY="${DISPLAY:-:0}"

# Limpar processos anteriores para evitar portas presas
"$SCRIPT_DIR/stop_core.sh" --session "$SESSION_DIR" >/dev/null 2>&1 || true

if [[ "$AUTO_MODE" -eq 1 ]]; then
    echo "[*] Modo BACKGROUND (--auto): a preparar Bootstrapper e nós intermédios..."

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

    if [[ "$WITH_STREAM" -eq 1 ]]; then
        # 3. Nó servidor (com stream automático apenas quando --with-stream é passado)
        echo "    -> A iniciar servidor $SERVER_NODE com stream '$STREAM_ID' ($VIDEO_FILE @ ${FPS}fps)..."
        vcmd -c "$SESSION_DIR/$SERVER_NODE" -- bash -c \
            "cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -u -m src.controlo.ott '$SERVER_NODE' '$BOOT_IP' '$TCP_PORT' --daemon --stream '$STREAM_ID' '$VIDEO_FILE' '$FPS' > '/tmp/esr_${SERVER_NODE}.log' 2>&1 &"
        sleep 2.0

        # 4. Nós clientes (com --play automático apenas quando --with-stream é passado)
        for client in $CLIENTS; do
            echo "    -> A iniciar cliente $client (--play $STREAM_ID)..."
            vcmd -c "$SESSION_DIR/$client" -- bash -c \
                "cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -u -m src.controlo.ott '$client' '$BOOT_IP' '$TCP_PORT' --daemon --play '$STREAM_ID' > '/tmp/esr_${client}.log' 2>&1 &"
        done

        echo ""
        echo "[OK] Overlay arrancado em modo automático com stream '$STREAM_ID'!"
    else
        echo ""
        echo "[OK] Bootstrapper e nós intermédios preparados em background (nenhum fluxo iniciado)!"
        echo "     Nos terminais do CORE, arranque o servidor e clientes manualmente:"
        echo "       Servidor ($SERVER_NODE): ./scripts/start_node.sh $SERVER_NODE"
        echo "       Clientes ($CLIENTS): ./scripts/start_node.sh <node_id>"
    fi

    echo "     Logs disponíveis em /tmp/esr_<node>.log (ex: tail -f /tmp/esr_bootstrapper.log)"
    echo "     Para parar tudo: ./scripts/stop_core.sh"
    exit 0
fi

# Modo interativo em terminais limpos (evita xterms bugados do CORE) — sem iniciar nenhum fluxo
if command -v gnome-terminal >/dev/null 2>&1; then
    echo "[*] A abrir janela gnome-terminal com separadores para cada nó..."
    TABS=()
    TABS+=(--tab --title="Boot ($BOOT_NODE)" -- bash -c "vcmd -c '$SESSION_DIR/$BOOT_NODE' -- bash -c \"cd '$PROJECT_DIR' && python3 -m src.controlo.server\"; exec bash")

    for node in $NODES; do
        if [[ "$node" == "$SERVER_NODE" ]]; then
            TABS+=(--tab --title="$node (Srv)" -- bash -c "sleep 1; vcmd -c '$SESSION_DIR/$node' -- bash -c \"cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -m src.controlo.ott '$node' '$BOOT_IP' '$TCP_PORT'\"; exec bash")
        else
            TABS+=(--tab --title="$node" -- bash -c "sleep 1.5; vcmd -c '$SESSION_DIR/$node' -- bash -c \"cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -m src.controlo.ott '$node' '$BOOT_IP' '$TCP_PORT'\"; exec bash")
        fi
    done

    gnome-terminal "${TABS[@]}" &
    echo "[OK] Overlay preparado em separadores (nenhum fluxo iniciado ainda)!"
    echo "     No servidor ($SERVER_NODE):     stream $STREAM_ID $VIDEO_FILE $FPS"
    echo "     Nos clientes ($CLIENTS):  play $STREAM_ID"
elif command -v xterm >/dev/null 2>&1; then
    echo "[*] A abrir janelas xterm para cada nó (gnome-terminal não instalado)..."
    xterm -T "Boot ($BOOT_NODE)" -e bash -c "vcmd -c '$SESSION_DIR/$BOOT_NODE' -- bash -c \"cd '$PROJECT_DIR' && python3 -m src.controlo.server\"; exec bash" &
    sleep 1
    for node in $NODES; do
        if [[ "$node" == "$SERVER_NODE" ]]; then
            xterm -T "$node (Srv)" -e bash -c "vcmd -c '$SESSION_DIR/$node' -- bash -c \"cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -m src.controlo.ott '$node' '$BOOT_IP' '$TCP_PORT'\"; exec bash" &
        else
            xterm -T "$node" -e bash -c "vcmd -c '$SESSION_DIR/$node' -- bash -c \"cd '$PROJECT_DIR' && DISPLAY='$DISPLAY' python3 -m src.controlo.ott '$node' '$BOOT_IP' '$TCP_PORT'\"; exec bash" &
        fi
        sleep 0.3
    done
    echo "[OK] Overlay preparado em janelas xterm (nenhum fluxo iniciado ainda)!"
    echo "     No servidor ($SERVER_NODE):     stream $STREAM_ID $VIDEO_FILE $FPS"
    echo "     Nos clientes ($CLIENTS):  play $STREAM_ID"
else
    echo "[AVISO] Nem 'gnome-terminal' nem 'xterm' encontrados. Use './scripts/launch_core.sh --auto'"
    echo "        ou abra cada nó e corra './scripts/start_node.sh <node_id>'."
    exit 1
fi
