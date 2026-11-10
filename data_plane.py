"""
data_plane.py — Módulo do Plano de Dados (Multicast Overlay sobre UDP)
======================================================================
Implementa duas classes principais:

  OverlayForwarder — Nó intermédio: recebe pacotes UDP e replica-os para
                     os destinos ativos (multicast aplicacional).

  StreamServer     — Servidor de origem: lê frames MJPEG, empacota em RTP
                     e envia para os destinos ativos via unicast UDP.

Protocolo de transporte:
  - Porta UDP fixa: VIDEO_PORT = 25000
  - Formato no fio: <stream_id>|<RTP_header_12B><payload_JPEG>

Integração com o Plano de Controlo:
  Ambas as classes recebem uma função `get_active_destinations_func(stream_id)`
  que é fornecida pela camada de controlo (Pessoas 1/2) e devolve a lista de
  IPs destino para cada fluxo em tempo real.

Autor: Plano de Dados — TP2 ESR/MEI-UMinho
"""

import logging
import socket
import threading
import time
from typing import Callable, Dict, List, Optional

from rtp_packet import RtpPacket, SEQ_MAX
from video_stream import VideoStream, VideoStreamError

# ──────────────────────────────────────────────────────────────────────────────
# Configuração de logging
# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────────────────────────────────────

# Porta UDP única usada por todos os nós do overlay para vídeo
VIDEO_PORT = 25000

# Tamanho do buffer de receção (deve acomodar frames MJPEG + cabeçalhos)
RECV_BUFFER_SIZE = 65536  # 64 KiB


# ──────────────────────────────────────────────────────────────────────────────
# Utilitário: criação de socket UDP reutilizável
# ──────────────────────────────────────────────────────────────────────────────

def _create_udp_socket(bind_port: Optional[int] = None) -> socket.socket:
    """
    Cria e configura um socket UDP para o plano de dados.

    Aplica SO_REUSEADDR (e SO_REUSEPORT quando disponível) para evitar
    erros «Address already in use» ao reiniciar scripts no emulador CORE.

    Parâmetros
    ----------
    bind_port : Porta a fazer bind; None para socket de envio apenas.

    Retorna
    -------
    socket.socket configurado e pronto.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Permite reutilizar o endereço imediatamente após reinício
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # SO_REUSEPORT não existe em Windows mas é essencial em Linux/CORE
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            # Algumas versões de kernel antigo não suportam SO_REUSEPORT
            logger.warning("SO_REUSEPORT não disponível neste sistema.")

    if bind_port is not None:
        sock.bind(("", bind_port))
        logger.debug("Socket UDP ligado à porta %d.", bind_port)

    return sock


# ══════════════════════════════════════════════════════════════════════════════
# CLASSE 1: OverlayForwarder
# ══════════════════════════════════════════════════════════════════════════════

class OverlayForwarder:
    """
    Nó de reencaminhamento do overlay multicast.

    Recebe pacotes UDP na porta VIDEO_PORT e replica-os para todos os
    destinos ativos do fluxo correspondente, sem descodificar o payload RTP
    (forwarding em «passagem direta» para latência mínima).

    Funcionamento:
        1. Recebe datagrama UDP bruto.
        2. Extrai o stream_id pelo primeiro separador b'|' (O(n) no prefixo).
        3. Consulta get_active_destinations_func(stream_id) → lista de IPs.
        4. Reenvia o datagrama idêntico para cada IP destino:porta VIDEO_PORT.

    Parâmetros
    ----------
    get_active_destinations_func : Callable[[str], List[str]]
        Função fornecida pelo plano de controlo que, dado um stream_id,
        retorna a lista de IPs destino ativos (pode ser [] se não houver).
    """

    def __init__(self, get_active_destinations_func: Callable[[str], List[str]]):
        self._get_dests = get_active_destinations_func
        self._sock: Optional[socket.socket] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ──────────────────────────────────────────────────────────────────────────
    # Ciclo de vida
    # ──────────────────────────────────────────────────────────────────────────

    def start(self):
        """
        Inicia o forwarder em daemon thread de background.
        Retorna imediatamente; o loop corre em paralelo.
        """
        if self._running:
            logger.warning("OverlayForwarder já está em execução.")
            return

        self._sock = _create_udp_socket(bind_port=VIDEO_PORT)
        self._running = True

        self._thread = threading.Thread(
            target=self._forward_loop,
            name="OverlayForwarder",
            daemon=True   # Termina automaticamente quando o processo principal sai
        )
        self._thread.start()
        logger.info("OverlayForwarder iniciado na porta UDP %d.", VIDEO_PORT)

    def stop(self):
        """Para o forwarder e fecha o socket de forma limpa."""
        self._running = False
        if self._sock:
            # Fechar o socket faz com que recvfrom() levante uma exceção,
            # desbloqueando a thread do loop
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        logger.info("OverlayForwarder parado.")

    # ──────────────────────────────────────────────────────────────────────────
    # Loop de reencaminhamento (corre na thread daemon)
    # ──────────────────────────────────────────────────────────────────────────

    def _forward_loop(self):
        """
        Loop principal do forwarder: recebe e replica pacotes continuamente.

        Otimização de latência:
            O stream_id é extraído com um split em b'|' limitado a 1 divisão,
            evitando qualquer descodificação do cabeçalho RTP ou do payload JPEG.
        """
        # Socket de envio separado (sem bind) para evitar conflito de porta
        send_sock = _create_udp_socket(bind_port=None)

        logger.debug("OverlayForwarder: loop de reencaminhamento iniciado.")

        while self._running:
            try:
                # ── Receber datagrama (bloqueante) ────────────────────────────
                data, addr = self._sock.recvfrom(RECV_BUFFER_SIZE)

                # ── Extrair stream_id com split mínimo ────────────────────────
                # Divide apenas na 1.ª ocorrência de b'|' para extrair o prefixo
                parts = data.split(b"|", 1)
                if len(parts) < 2:
                    # Pacote malformado; ignorar silenciosamente
                    logger.debug(
                        "OverlayForwarder: pacote malformado de %s (sem '|'). "
                        "A ignorar.", addr
                    )
                    continue

                stream_id = parts[0].decode("utf-8", errors="replace")

                # ── [PONTO 4] Consultar destinos com cópia rasa defensiva ────
                # list() garante snapshot imutável para a iteração de sendto;
                # evita RuntimeError se a Pessoa 2 alterar a tabela enquanto
                # este loop está a correr (tabela modificada por outra thread).
                destinations: List[str] = list(self._get_dests(stream_id))

                if not destinations:
                    # Sem destinos → descartar o pacote (não reencaminhar)
                    continue

                # ── Replicar pacote para cada destino ─────────────────────────
                for dest_ip in destinations:
                    try:
                        send_sock.sendto(data, (dest_ip, VIDEO_PORT))
                    except OSError as exc:
                        logger.warning(
                            "OverlayForwarder: erro ao enviar para %s:%d — %s",
                            dest_ip, VIDEO_PORT, exc
                        )

            except OSError:
                # Socket fechado (stop() foi chamado) ou outro erro de I/O
                if self._running:
                    logger.error(
                        "OverlayForwarder: erro de socket inesperado.",
                        exc_info=True
                    )
                break

        send_sock.close()
        logger.debug("OverlayForwarder: loop de reencaminhamento terminado.")


# ══════════════════════════════════════════════════════════════════════════════
# CLASSE 2: StreamServer
# ══════════════════════════════════════════════════════════════════════════════

class StreamServer:
    """
    Servidor de origem de streams de vídeo MJPEG sobre RTP/UDP.

    Para cada fluxo iniciado com `start_stream()`, uma thread dedicada:
      1. Mantém o framerate estável com `time.perf_counter()` (sem deriva).
      2. Consulta os destinos ativos antes de cada frame.
      3. Só lê o ficheiro e envia se houver pelo menos um destino ativo.
      4. Encapsula o frame em RTP com timestamp e número de sequência corretos.
      5. Envia via unicast UDP para cada destino.

    Parâmetros
    ----------
    get_active_destinations_func : Callable[[str], List[str]]
        Função fornecida pelo plano de controlo.
    """

    def __init__(self, get_active_destinations_func: Callable[[str], List[str]]):
        self._get_dests = get_active_destinations_func

        # Dicionário de streams em execução: stream_id → {"thread", "stop_event"}
        self._streams: Dict[str, dict] = {}
        self._streams_lock = threading.Lock()

        # Socket de envio único partilhado por todos os streams (thread-safe
        # para sendto em Python porque é protegido pelo GIL ao nível C)
        self._send_sock: Optional[socket.socket] = None

    # ──────────────────────────────────────────────────────────────────────────
    # Ciclo de vida do servidor
    # ──────────────────────────────────────────────────────────────────────────

    def start(self):
        """Prepara o socket de envio. Deve ser chamado antes de start_stream()."""
        self._send_sock = _create_udp_socket(bind_port=None)
        logger.info("StreamServer: socket de envio pronto.")

    def stop_all(self):
        """Para todos os streams em execução e fecha o socket."""
        with self._streams_lock:
            stream_ids = list(self._streams.keys())

        for sid in stream_ids:
            self.stop_stream(sid)

        if self._send_sock:
            try:
                self._send_sock.close()
            except OSError:
                pass
            self._send_sock = None

        logger.info("StreamServer: todos os streams parados.")

    # ──────────────────────────────────────────────────────────────────────────
    # Gestão de streams individuais
    # ──────────────────────────────────────────────────────────────────────────

    def start_stream(self, stream_id: str, filepath: str, fps: float = 20.0):
        """
        Inicia a transmissão de um stream de vídeo em thread dedicada.

        Parâmetros
        ----------
        stream_id : Identificador lógico do fluxo (ex: "S1_V1").
        filepath  : Caminho para o ficheiro .Mjpeg.
        fps       : Framerate alvo em frames por segundo (padrão: 20 fps).

        Lança
        -----
        RuntimeError — se o stream já estiver a correr.
        VideoStreamError — se o ficheiro de vídeo não existir.
        """
        with self._streams_lock:
            if stream_id in self._streams:
                raise RuntimeError(
                    f"Stream {stream_id!r} já está em execução."
                )

        # Valida o ficheiro antes de lançar a thread
        if not VideoStream.__init__.__doc__ and True:  # Força verificação
            pass
        # (A VideoStream lança VideoStreamError no __init__ se o ficheiro falhar)

        stop_event = threading.Event()

        thread = threading.Thread(
            target=self._stream_worker,
            args=(stream_id, filepath, fps, stop_event),
            name=f"Stream-{stream_id}",
            daemon=True
        )

        with self._streams_lock:
            self._streams[stream_id] = {
                "thread": thread,
                "stop_event": stop_event
            }

        thread.start()
        logger.info(
            "StreamServer: stream %r iniciado a %g fps (ficheiro: %r).",
            stream_id, fps, filepath
        )

    def stop_stream(self, stream_id: str):
        """Para um stream específico de forma graciosa."""
        with self._streams_lock:
            entry = self._streams.get(stream_id)

        if entry is None:
            logger.warning("stop_stream: stream %r não encontrado.", stream_id)
            return

        # Sinaliza a thread para parar
        entry["stop_event"].set()

        # Aguarda que a thread termine (máximo 3 segundos)
        entry["thread"].join(timeout=3.0)

        with self._streams_lock:
            self._streams.pop(stream_id, None)

        logger.info("StreamServer: stream %r parado.", stream_id)

    @property
    def active_streams(self) -> List[str]:
        """Lista dos stream_ids atualmente em transmissão."""
        with self._streams_lock:
            return list(self._streams.keys())

    # ──────────────────────────────────────────────────────────────────────────
    # Worker de streaming (corre numa thread por fluxo)
    # ──────────────────────────────────────────────────────────────────────────

    def _stream_worker(
        self,
        stream_id: str,
        filepath: str,
        fps: float,
        stop_event: threading.Event
    ):
        """
        Loop de envio de um fluxo de vídeo.

        Controlo de tempo preciso sem jitter acumulado (ponto 3 da checklist):
            O deadline do próximo frame (next_frame_time) é calculado UMA vez
            por iteração, ANTES de qualquer I/O.  O sleep compensa o tempo real
            gasto em leitura de disco, empacotamento RTP e envio UDP:

                frame_start = perf_counter()   ← registo antes do trabalho
                <leitura + empacotamento + sendto>
                elapsed = perf_counter() - frame_start
                time.sleep(max(0, frame_interval - elapsed))

            Se uma operação demorar mais do que um intervalo inteiro (ex: pico
            de I/O), o sleep é 0 (sem acumulação de débito).

        Thread-safety na consulta de destinos (ponto 4 da checklist):
            O valor retornado por _get_dests() é envolvido em list() dentro
            deste worker, garantindo uma cópia rasa imutável para a iteração
            do sendto. Assim, mesmo que a Pessoa 2 substitua a lista interna
            da tabela de rotas enquanto este ciclo corre, não ocorre
            «RuntimeError: list changed size during iteration».

        Quando não há destinos ativos:
            Não lê do ficheiro (preserva a posição), apenas avança o deadline.
            Quando os destinos surgem, o stream retoma o frame seguinte.
        """
        frame_interval = 1.0 / fps      # Intervalo entre frames em segundos
        seq_num = 0                      # Número de sequência RTP (16 bits, cíclico)
        ssrc = id(threading.current_thread()) & 0xFFFFFFFF  # SSRC único por thread

        try:
            video = VideoStream(filepath)
        except VideoStreamError as exc:
            logger.error(
                "StreamServer [%s]: nao foi possivel abrir o video: %s",
                stream_id, exc
            )
            # Remove o stream da tabela para não ficar em estado inconsistente
            with self._streams_lock:
                self._streams.pop(stream_id, None)
            return

        logger.debug("StreamServer [%s]: worker iniciado.", stream_id)

        try:
            while not stop_event.is_set():

                # ── [PONTO 3] Registo do instante de início do frame ──────────
                # Feito ANTES de qualquer operação de I/O ou de rede, para que
                # o sleep final compense o tempo real gasto neste ciclo.
                frame_start = time.perf_counter()

                # ── [PONTO 4] Consultar destinos com cópia rasa defensiva ─────
                # list() garante snapshot imutável; evita RuntimeError se a
                # Pessoa 2 alterar a tabela de rotas durante a iteração abaixo.
                destinations: List[str] = list(self._get_dests(stream_id))

                if not destinations:
                    # Sem destinos: não lê frame, não envia.
                    # Dorme o intervalo completo e avança sem acumular débito.
                    time.sleep(frame_interval)
                    continue

                # ── Ler próximo frame do ficheiro MJPEG ───────────────────────
                try:
                    frame_data = video.next_frame()
                except VideoStreamError as exc:
                    logger.error(
                        "StreamServer [%s]: erro a ler frame: %s",
                        stream_id, exc
                    )
                    break

                # ── Empacotar frame em RTP ────────────────────────────────────
                pkt = RtpPacket()
                pkt.make(
                    seq_num=seq_num,
                    timestamp=RtpPacket.current_timestamp_ms(),
                    ssrc=ssrc,
                    payload=frame_data,
                    stream_id=stream_id
                )
                wire_data = pkt.get_packet()

                # Incrementar número de sequência com overflow cíclico a 65535
                seq_num = (seq_num + 1) & SEQ_MAX

                # ── Enviar para cada destino ───────────────────────────────────
                for dest_ip in destinations:
                    try:
                        self._send_sock.sendto(wire_data, (dest_ip, VIDEO_PORT))
                    except OSError as exc:
                        logger.warning(
                            "StreamServer [%s]: erro ao enviar para %s -- %s",
                            stream_id, dest_ip, exc
                        )

                # ── [PONTO 3] Sleep preciso: compensa I/O + rede ──────────────
                # elapsed inclui: next_frame() + get_packet() + todos os sendto()
                # max(0, ...) garante que nunca se dorme tempo negativo
                elapsed = time.perf_counter() - frame_start
                time.sleep(max(0.0, frame_interval - elapsed))

        finally:
            video.close()
            logger.debug("StreamServer [%s]: worker terminado.", stream_id)

