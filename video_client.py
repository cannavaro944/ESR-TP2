"""
video_client.py — Cliente de vídeo com interface gráfica Tkinter
================================================================
Recebe frames de vídeo via UDP/RTP e apresenta-os em tempo real
numa janela Tkinter.

Uso:
    python video_client.py <stream_id> [--host HOST] [--callback]

Exemplos:
    python video_client.py S1_V1
    python video_client.py S1_V2 --host 10.0.0.1

Arquitetura interna:
    - Thread principal  → loop Tkinter (GUI)
    - Thread de escuta  → recebe UDP, descodifica RTP, envia frame para GUI
    - Comunicação entre threads via queue.Queue (thread-safe, sem locks manuais)

Protocolo de encerramento:
    Ao fechar a janela, invoca on_close(stream_id) que pode ser ligada
    ao módulo de controlo para enviar uma mensagem LEAVE e desativar a rota.

Autor: Plano de Dados — TP2 ESR/MEI-UMinho
"""

import io
import logging
import queue
import socket
import sys
import threading
import tkinter as tk
from typing import Callable, Optional

# PIL é necessário para converter bytes JPEG em imagem Tkinter
try:
    from PIL import Image, ImageTk
except ImportError:
    print(
        "[ERRO] A biblioteca Pillow não está instalada.\n"
        "       Instale com: pip install Pillow",
        file=sys.stderr
    )
    sys.exit(1)

from rtp_packet import RtpPacket
from data_plane import VIDEO_PORT, RECV_BUFFER_SIZE

# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# Timeout do socket de receção em segundos (evita bloqueio eterno no shutdown)
SOCKET_TIMEOUT = 0.5

# Dimensões padrão da janela de vídeo (pixels)
DEFAULT_WIDTH  = 640
DEFAULT_HEIGHT = 480

# Intervalo de polling da queue na GUI (milissegundos)
GUI_POLL_INTERVAL_MS = 15   # ≈ 66 Hz → suficiente para 20-30 fps


# ══════════════════════════════════════════════════════════════════════════════
# Classe principal do cliente
# ══════════════════════════════════════════════════════════════════════════════

class VideoClient:
    """
    Cliente de vídeo RTP com interface gráfica Tkinter.

    Parâmetros
    ----------
    stream_id   : Identificador do fluxo a receber (ex: "S1_V1").
    listen_host : Endereço IP local a escutar (padrão: "" = todas as interfaces).
    listen_port : Porta UDP local a escutar (padrão: VIDEO_PORT = 25000).
    on_close    : Callback opcional invocado com (stream_id) ao fechar a janela.
                  Pode ser usado para enviar mensagem LEAVE ao plano de controlo.
    """

    def __init__(
        self,
        stream_id: str,
        listen_host: str = "",
        listen_port: int = VIDEO_PORT,
        on_close: Optional[Callable[[str], None]] = None
    ):
        self.stream_id   = stream_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        self._on_close   = on_close

        # Contadores de estatísticas
        self._frames_received = 0
        self._frames_displayed = 0
        self._bytes_received = 0

        # Controlo de estado
        self._running = False
        self._sock: Optional[socket.socket] = None
        self._listen_thread: Optional[threading.Thread] = None

        # Queue inter-thread: a thread de escuta coloca ImageTk.PhotoImage aqui;
        # a GUI consome com after() sem bloquear o event loop
        self._frame_queue: queue.Queue = queue.Queue(maxsize=5)

        # ── Construção da interface gráfica ───────────────────────────────────
        self._root = tk.Tk()
        self._root.title(f"ESR Video Client — Stream: {stream_id}")
        self._root.resizable(False, False)

        # Painel de vídeo (Label com imagem)
        self._video_label = tk.Label(
            self._root,
            bg="black",
            width=DEFAULT_WIDTH,
            height=DEFAULT_HEIGHT
        )
        self._video_label.pack()

        # Barra de estado
        self._status_var = tk.StringVar(value="A aguardar frames...")
        status_bar = tk.Label(
            self._root,
            textvariable=self._status_var,
            bd=1,
            relief=tk.SUNKEN,
            anchor=tk.W,
            font=("Courier", 9)
        )
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        # Protocolo de fecho da janela
        self._root.protocol("WM_DELETE_WINDOW", self._on_window_close)

    # ──────────────────────────────────────────────────────────────────────────
    # Arranque e paragem
    # ──────────────────────────────────────────────────────────────────────────

    def run(self):
        """
        Inicia o socket, a thread de escuta e o loop Tkinter.
        Bloqueia até que a janela seja fechada.
        """
        self._running = True

        # Abrir socket UDP
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass

        self._sock.settimeout(SOCKET_TIMEOUT)
        self._sock.bind((self.listen_host, self.listen_port))
        logger.info(
            "VideoClient [%s]: socket ligado à porta %d.",
            self.stream_id, self.listen_port
        )

        # Lançar thread de escuta (daemon: termina com o processo)
        self._listen_thread = threading.Thread(
            target=self._listen_loop,
            name=f"Listen-{self.stream_id}",
            daemon=True
        )
        self._listen_thread.start()

        # Agendar o primeiro poll da queue na GUI
        self._root.after(GUI_POLL_INTERVAL_MS, self._poll_frame_queue)

        # Entrar no event loop Tkinter (bloqueia aqui até fechar a janela)
        self._root.mainloop()

    def _stop(self):
        """Para a thread de escuta e fecha o socket."""
        self._running = False

        # Fechar socket → desbloqueia recvfrom() na thread de escuta
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

        if self._listen_thread and self._listen_thread.is_alive():
            self._listen_thread.join(timeout=2.0)

        logger.info(
            "VideoClient [%s]: parado. Frames recebidos: %d, exibidos: %d.",
            self.stream_id, self._frames_received, self._frames_displayed
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Thread de escuta UDP (background)
    # ──────────────────────────────────────────────────────────────────────────

    def _listen_loop(self):
        """
        Recebe datagramas UDP, descodifica RTP e coloca frames na queue.

        Filtra pacotes pelo stream_id para ignorar fluxos de outros canais
        (caso múltiplos streams partilhem a mesma porta no mesmo host).
        """
        logger.debug("VideoClient [%s]: thread de escuta iniciada.", self.stream_id)

        while self._running:
            try:
                data, addr = self._sock.recvfrom(RECV_BUFFER_SIZE)

                # ── Verificação rápida do stream_id sem descodificar RTP ──────
                # (Para descartar pacotes de outros streams com custo mínimo)
                sep_idx = data.find(b"|")
                if sep_idx < 0:
                    continue  # Pacote malformado

                pkt_stream_id = data[:sep_idx].decode("utf-8", errors="replace")
                if pkt_stream_id != self.stream_id:
                    continue  # Não é o nosso stream

                # ── Descodificar pacote RTP completo ──────────────────────────
                try:
                    pkt = RtpPacket.decode(data)
                except (ValueError, Exception) as exc:
                    logger.warning(
                        "VideoClient [%s]: erro a descodificar RTP: %s",
                        self.stream_id, exc
                    )
                    continue

                self._frames_received += 1
                self._bytes_received += len(data)

                # ── Converter payload JPEG → ImageTk para a GUI ───────────────
                try:
                    img = Image.open(io.BytesIO(pkt.payload))
                    img = img.resize((DEFAULT_WIDTH, DEFAULT_HEIGHT), Image.LANCZOS)
                    photo = ImageTk.PhotoImage(img)
                except Exception as exc:
                    logger.warning(
                        "VideoClient [%s]: erro a descodificar imagem JPEG: %s",
                        self.stream_id, exc
                    )
                    continue

                # ── Colocar na queue (descarta se a GUI está atrasada) ─────────
                try:
                    # put_nowait: não bloqueia; descarta o frame se a queue estiver cheia
                    self._frame_queue.put_nowait((photo, pkt.seq_num))
                except queue.Full:
                    # GUI está ocupada; descartar frame mais antigo e inserir novo
                    try:
                        self._frame_queue.get_nowait()
                    except queue.Empty:
                        pass
                    try:
                        self._frame_queue.put_nowait((photo, pkt.seq_num))
                    except queue.Full:
                        pass

            except socket.timeout:
                # Timeout normal — volta a recvfrom() para verificar _running
                continue
            except OSError:
                # Socket fechado (stop foi chamado) ou erro de I/O
                if self._running:
                    logger.error(
                        "VideoClient [%s]: erro de socket inesperado.",
                        self.stream_id, exc_info=True
                    )
                break

        logger.debug("VideoClient [%s]: thread de escuta terminada.", self.stream_id)

    # ──────────────────────────────────────────────────────────────────────────
    # Polling da queue na thread GUI (Tkinter after())
    # ──────────────────────────────────────────────────────────────────────────

    def _poll_frame_queue(self):
        """
        Consome frames da queue e atualiza o Label de vídeo.
        Chamado periodicamente pelo event loop Tkinter via after().
        """
        try:
            photo, seq_num = self._frame_queue.get_nowait()

            # Atualizar imagem no Label
            # NOTA: é necessário guardar referência à imagem para evitar garbage
            # collection pelo Python (limitação do Tkinter com PhotoImage)
            self._video_label.configure(image=photo)
            self._video_label._photo_ref = photo  # Manter referência viva

            self._frames_displayed += 1

            # Atualizar barra de estado
            self._status_var.set(
                f"Stream: {self.stream_id}  |  "
                f"Seq: {seq_num:05d}  |  "
                f"Frames exibidos: {self._frames_displayed:6d}  |  "
                f"Recebidos: {self._frames_received:6d}  |  "
                f"Bytes: {self._bytes_received:,}"
            )

        except queue.Empty:
            pass  # Sem novo frame disponível neste ciclo

        # Re-agendar o próximo poll (se ainda em execução)
        if self._running:
            self._root.after(GUI_POLL_INTERVAL_MS, self._poll_frame_queue)

    # ──────────────────────────────────────────────────────────────────────────
    # Protocolo de fecho da janela
    # ──────────────────────────────────────────────────────────────────────────

    def _on_window_close(self):
        """
        Invocado pelo Tkinter quando o utilizador fecha a janela (botão X).

        Sequência de encerramento:
            1. Para a thread de escuta e fecha o socket.
            2. Destrói a janela Tkinter.
            3. Invoca o callback on_close(stream_id) (ex: enviar LEAVE ao nó de controlo).
        """
        logger.info(
            "VideoClient [%s]: janela fechada pelo utilizador.",
            self.stream_id
        )
        self._stop()
        self._root.destroy()

        # Invocar callback externo (ex: desativar rota no plano de controlo)
        if self._on_close:
            try:
                self._on_close(self.stream_id)
            except Exception as exc:
                logger.error(
                    "VideoClient [%s]: erro no callback on_close: %s",
                    self.stream_id, exc
                )

    # ──────────────────────────────────────────────────────────────────────────
    # Estatísticas
    # ──────────────────────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict:
        """Retorna dicionário com estatísticas de receção."""
        return {
            "stream_id"       : self.stream_id,
            "frames_received" : self._frames_received,
            "frames_displayed": self._frames_displayed,
            "bytes_received"  : self._bytes_received,
        }


# ══════════════════════════════════════════════════════════════════════════════
# Ponto de entrada (execução direta)
# ══════════════════════════════════════════════════════════════════════════════

def _default_on_close(stream_id: str):
    """
    Callback padrão de encerramento (quando não há integração com controlo).
    Apenas regista o evento no log.
    """
    logger.info(
        "on_close chamado para stream %r. "
        "(Integrar com Pessoa 2 para enviar mensagem LEAVE.)",
        stream_id
    )


def main():
    """Ponto de entrada principal: parseia argumentos e lança o cliente."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Cliente de vídeo ESR — Plano de Dados TP2"
    )
    parser.add_argument(
        "stream_id",
        help='Identificador do stream a receber (ex: "S1_V1")'
    )
    parser.add_argument(
        "port",
        nargs="?",
        type=int,
        default=VIDEO_PORT,
        help=f"Porta UDP a escutar (padrão: {VIDEO_PORT})"
    )
    parser.add_argument(
        "--port",
        dest="port_flag",
        type=int,
        default=None,
        help=f"Porta UDP alternativa (sobrepõe o argumento posicional)"
    )
    parser.add_argument(
        "--host",
        default="",
        help="Interface local a escutar (padrão: todas as interfaces)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de logging (padrão: INFO)"
    )

    args = parser.parse_args()
    listen_port = args.port_flag if args.port_flag is not None else args.port

    # Configurar logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S"
    )

    client = VideoClient(
        stream_id=args.stream_id,
        listen_host=args.host,
        listen_port=listen_port,
        on_close=_default_on_close
    )

    try:
        client.run()
    except KeyboardInterrupt:
        print("\n[INFO] Cliente interrompido pelo utilizador.")


if __name__ == "__main__":
    main()

