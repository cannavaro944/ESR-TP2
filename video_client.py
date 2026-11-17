"""
video_client_cli.py — Cliente de vídeo SEM interface gráfica
================================================================
Recebe pacotes de vídeo via UDP/RTP e imprime estatísticas na
consola em tempo real. Adaptado para correr no emulador CORE
sem bibliotecas gráficas.
"""

import io
import logging
import queue
import socket
import sys
import threading
import time
from typing import Callable, Optional

# --- BLOCO GRÁFICO COMENTADO ---
# try:
#     from PIL import Image, ImageTk
# except ImportError:
#     pass
# import tkinter as tk

from rtp_packet import RtpPacket
from data_plane import VIDEO_PORT, RECV_BUFFER_SIZE

# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
SOCKET_TIMEOUT = 0.5
DEFAULT_WIDTH  = 640
DEFAULT_HEIGHT = 480


class VideoClient:
    """
    Cliente de vídeo RTP em modo texto (CLI).
    """

    def __init__(
        self,
        stream_id: str,
        listen_host: str = "",
        on_close: Optional[Callable[[str], None]] = None
    ):
        self.stream_id   = stream_id
        self.listen_host = listen_host
        self._on_close   = on_close

        # Contadores de estatísticas
        self._frames_received = 0
        self._bytes_received = 0

        self._running = False
        self._sock: Optional[socket.socket] = None
        self._listen_thread: Optional[threading.Thread] = None


    def run(self):
        """Inicia a escuta no terminal. Bloqueia até ser interrompido (Ctrl+C)."""
        self._running = True

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass

        self._sock.settimeout(SOCKET_TIMEOUT)
        
        # --- ATENÇÃO À PORTA DE ESCUTA ---
        # Se fores testar com a nossa alteração do ott.py, muda aqui o VIDEO_PORT para 25001,
        # ou passa a porta por argumento.
        porta_escuta = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else VIDEO_PORT
        
        self._sock.bind((self.listen_host, porta_escuta))
        print(f"\n[*] VideoClient CLI: À escuta na porta UDP {porta_escuta} para o fluxo '{self.stream_id}'...\n")

        self._listen_thread = threading.Thread(
            target=self._listen_loop,
            name=f"Listen-{self.stream_id}",
            daemon=True
        )
        self._listen_thread.start()

        # Mantém a thread principal viva para receber Ctrl+C
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self._stop()

    def _stop(self):
        """Para a thread de escuta."""
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

        print(f"\n\n[*] VideoClient parado. Total frames recebidos: {self._frames_received}")
        if self._on_close:
            self._on_close(self.stream_id)


    def _listen_loop(self):
        """Recebe datagramas UDP, descodifica RTP e regista na consola."""
        while self._running:
            try:
                data, addr = self._sock.recvfrom(RECV_BUFFER_SIZE)

                # ── Verificação rápida do stream_id ──────
                sep_idx = data.find(b"|")
                if sep_idx < 0:
                    continue

                pkt_stream_id = data[:sep_idx].decode("utf-8", errors="replace")
                if pkt_stream_id != self.stream_id:
                    continue

                # ── Descodificar pacote RTP ──────────────
                try:
                    pkt = RtpPacket.decode(data)
                except (ValueError, Exception) as exc:
                    continue

                self._frames_received += 1
                self._bytes_received += len(data)

                # ── LOG VISUAL DA RECEÇÃO ────────────────
                print(f"  [>] Recebido pacote UDP (Seq: {pkt.seq_num:05d}) | Total: {self._frames_received} frames | Bytes: {self._bytes_received:,}")

            except socket.timeout:
                continue
            except OSError:
                break


def _default_on_close(stream_id: str):
    pass

def main():
    if len(sys.argv) < 2:
        print("Uso: python video_client.py <stream_id> [porta_local]")
        sys.exit(1)

    stream_id = sys.argv[1]
    
    client = VideoClient(
        stream_id=stream_id,
        on_close=_default_on_close
    )

    client.run()

if __name__ == "__main__":
    main()
