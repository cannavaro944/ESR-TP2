"""
scripts/demo_video_local.py — Demonstração local rápida do streaming de vídeo
==============================================================================
Arranca o StreamServer em localhost e abre o VideoClient (subprocesso real)
para testar visualmente a reprodução de vídeo num só comando.

Quando fechar a janela do vídeo, o servidor pára automaticamente.

Uso:
  python scripts/demo_video_local.py
  python scripts/demo_video_local.py --video assets/movie.Mjpeg --fps 20
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.dados.data_plane import StreamServer, VIDEO_PORT


def main():
    parser = argparse.ArgumentParser(
        description="Teste visual local (StreamServer + VideoClient)"
    )
    parser.add_argument(
        "--stream",
        default="Stream1",
        help="ID do fluxo (padrão: Stream1)",
    )
    parser.add_argument(
        "--video",
        default=str(ROOT_DIR / "assets" / "movie.Mjpeg"),
        help="Caminho para o ficheiro .Mjpeg (padrão: assets/movie.Mjpeg)",
    )
    parser.add_argument(
        "--fps",
        type=float,
        default=20.0,
        help="Framerates em fps (padrão: 20.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=VIDEO_PORT,
        help=f"Porta UDP do cliente (padrão: {VIDEO_PORT})",
    )
    args = parser.parse_args()

    if args.port != VIDEO_PORT:
        print(
            f"[AVISO] O StreamServer envia para a porta {VIDEO_PORT}; "
            f"a iniciar cliente na porta {VIDEO_PORT} para receção direta."
        )
        args.port = VIDEO_PORT

    server = StreamServer(lambda sid: ["127.0.0.1"] if sid == args.stream else [])
    server.start()
    server.start_stream(args.stream, args.video, fps=args.fps)

    print(f"[*] StreamServer a transmitir '{args.stream}' ({args.video} @ {args.fps} fps)...")
    print("[*] A abrir VideoClient. Feche a janela do vídeo para terminar.")

    proc = subprocess.Popen(
        [sys.executable, "-m", "src.dados.video_client", args.stream, "--port", str(args.port)],
        cwd=str(ROOT_DIR),
    )

    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
    finally:
        server.stop_all()
        print("[*] Demonstração terminada.")


if __name__ == "__main__":
    main()
