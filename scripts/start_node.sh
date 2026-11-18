#!/usr/bin/env bash
# Atalho para executar manualmente dentro do terminal de um nó no CORE.
# Uso:
#   ./scripts/start_node.sh boot                  # Arranca o bootstrapper (server.py)
#   ./scripts/start_node.sh S1                    # Arranca nó S1 (liga a 10.0.19.21:9000, porta 9001)
#   ./scripts/start_node.sh S1 --stream Stream1 assets/movie.Mjpeg 20
#   ./scripts/start_node.sh n20 10.0.19.21 9001 --play Stream1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

if [[ $# -lt 1 ]]; then
    echo "Uso: $0 <boot|node_id> [bootstrapper_ip=10.0.19.21] [listen_port=9001] [flags_extra...]"
    exit 1
fi

NODE_ID="$1"
shift

if [[ "$NODE_ID" == "boot" || "$NODE_ID" == "bootstrapper" ]]; then
    echo "[*] A iniciar Bootstrapper (src.controlo.server)..."
    exec python3 -m src.controlo.server
fi

BOOT_IP="${1:-10.0.19.21}"
if [[ $# -ge 1 && "$1" != --* ]]; then
    shift
else
    BOOT_IP="10.0.19.21"
fi

LISTEN_PORT="9001"
if [[ $# -ge 1 && "$1" != --* ]]; then
    LISTEN_PORT="$1"
    shift
fi

export DISPLAY="${DISPLAY:-:0}"
echo "[*] A iniciar nó '$NODE_ID' (bootstrapper=$BOOT_IP, porta_tcp=$LISTEN_PORT)..."
exec python3 -m src.controlo.ott "$NODE_ID" "$BOOT_IP" "$LISTEN_PORT" "$@"
