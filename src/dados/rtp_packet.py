"""
rtp_packet.py — Módulo de encapsulamento/desencapsulamento RTP
=============================================================
Implementa o cabeçalho RTP padrão de 12 bytes (RFC 3550) com suporte
a framing de Stream ID para identificação de fluxos de vídeo multicast.

Formato do pacote no fio (wire format):
    <stream_id>|<RTP_header_12bytes><payload_jpeg>

Utilizado por:
    - StreamServer  (data_plane.py) para empacotar frames JPEG
    - OverlayForwarder (data_plane.py) para reencaminhamento sem descodificação
    - VideoClient   (video_client.py) para descodificar e renderizar frames

Autor: Plano de Dados — TP2 ESR/MEI-UMinho
"""

import struct
import time


# ──────────────────────────────────────────────────────────────────────────────
# Constantes do cabeçalho RTP
# ──────────────────────────────────────────────────────────────────────────────
RTP_VERSION   = 2       # Versão RTP obrigatória
RTP_PADDING   = 0       # Sem padding
RTP_EXTENSION = 0       # Sem extensão de cabeçalho
RTP_CC        = 0       # Sem CSRCs
RTP_MARKER    = 0       # Sem marcação de limite de talk-spurt
RTP_PT_JPEG   = 26      # Payload Type 26 → JPEG (RFC 2435)

# Máximo do número de sequência (16 bits → overflow cíclico em 65535)
SEQ_MAX = 0xFFFF


class RtpPacket:
    """
    Representa um pacote RTP com payload JPEG e prefixo de Stream ID.

    Estrutura do cabeçalho (12 bytes, big-endian):
    ┌─────┬─────┬─────────┬──────────────────┐
    │ V P │X CC │ M  PT   │  Sequence Number  │  (bytes 0-3)
    ├─────┴─────┴─────────┴──────────────────┤
    │              Timestamp (ms)             │  (bytes 4-7)
    ├─────────────────────────────────────────┤
    │          SSRC (Sync Source ID)          │  (bytes 8-11)
    └─────────────────────────────────────────┘
    """

    HEADER_SIZE = 12  # Tamanho fixo do cabeçalho RTP em bytes

    def __init__(self):
        """Inicializa um pacote RTP vazio."""
        # Campos do cabeçalho
        self.version    = RTP_VERSION
        self.padding    = RTP_PADDING
        self.extension  = RTP_EXTENSION
        self.cc         = RTP_CC
        self.marker     = RTP_MARKER
        self.pt         = RTP_PT_JPEG
        self.seq_num    = 0
        self.timestamp  = 0
        self.ssrc       = 0

        # Dados
        self.payload    = b""    # Bytes JPEG do frame
        self.stream_id  = ""     # Identificador lógico do fluxo (ex: "S1_V1")

    # ──────────────────────────────────────────────────────────────────────────
    # Criação de pacote
    # ──────────────────────────────────────────────────────────────────────────

    def make(self, seq_num: int, timestamp: int, ssrc: int,
             payload: bytes, stream_id: str) -> "RtpPacket":
        """
        Preenche os campos do pacote com os valores fornecidos.

        Parâmetros
        ----------
        seq_num   : Número de sequência [0, 65535] (overflow cíclico tratado externamente).
        timestamp : Timestamp em milissegundos desde o início do stream.
        ssrc      : Identificador da fonte de sincronização.
        payload   : Bytes do frame JPEG.
        stream_id : Identificador lógico do fluxo (ex: "S1_V1").

        Retorna
        -------
        self — permite encadeamento: pkt.make(...).get_packet()
        """
        self.seq_num   = seq_num & SEQ_MAX   # Garante 16 bits (overflow cíclico)
        self.timestamp = timestamp & 0xFFFFFFFF  # Garante 32 bits
        self.ssrc      = ssrc & 0xFFFFFFFF
        self.payload   = payload
        self.stream_id = stream_id
        return self

    # ──────────────────────────────────────────────────────────────────────────
    # Serialização → bytes para envio via UDP
    # ──────────────────────────────────────────────────────────────────────────

    def get_packet(self) -> bytes:
        """
        Serializa o pacote para bytes prontos a enviar via UDP.

        Formato no fio:
            <stream_id>|<byte0><byte1><seq_num_16b><timestamp_32b><ssrc_32b><payload>

        O prefixo «stream_id|» permite ao OverlayForwarder identificar
        o fluxo com um simples split em b'|' sem descodificar o cabeçalho RTP.

        Retorna
        -------
        bytes — sequência completa pronta a passar ao socket.sendto()
        """
        # Byte 0: V(2b) | P(1b) | X(1b) | CC(4b)
        byte0 = (self.version  << 6) | \
                (self.padding  << 5) | \
                (self.extension << 4) | \
                (self.cc & 0x0F)

        # Byte 1: M(1b) | PT(7b)
        byte1 = (self.marker << 7) | (self.pt & 0x7F)

        # Cabeçalho de 12 bytes em big-endian
        header = struct.pack(
            "!BBHII",          # !  → big-endian
            byte0,             # B  → byte0 (flags + CC)
            byte1,             # B  → byte1 (marker + PT)
            self.seq_num,      # H  → Sequence Number (16 bits unsigned)
            self.timestamp,    # I  → Timestamp (32 bits unsigned)
            self.ssrc          # I  → SSRC (32 bits unsigned)
        )

        # Prefixo de stream ID + cabeçalho + payload
        prefix = f"{self.stream_id}|".encode("utf-8")
        return prefix + header + self.payload

    # ──────────────────────────────────────────────────────────────────────────
    # Desserialização ← bytes recebidos via UDP
    # ──────────────────────────────────────────────────────────────────────────

    @classmethod
    def decode(cls, byte_data: bytes) -> "RtpPacket":
        """
        Desserializa bytes recebidos do socket num objeto RtpPacket.

        Parâmetros
        ----------
        byte_data : Dados brutos recebidos do socket UDP (inclui prefixo stream_id).

        Retorna
        -------
        RtpPacket preenchido.

        Lança
        -----
        ValueError — se os dados estiverem malformados ou forem demasiado curtos.
        """
        pkt = cls()

        # 1. Extrair prefixo «stream_id|»
        sep_idx = byte_data.index(b"|")          # Posição do separador '|'
        pkt.stream_id = byte_data[:sep_idx].decode("utf-8")
        rtp_bytes = byte_data[sep_idx + 1:]      # Restante = cabeçalho RTP + payload

        if len(rtp_bytes) < cls.HEADER_SIZE:
            raise ValueError(
                f"Pacote RTP demasiado curto: {len(rtp_bytes)} bytes "
                f"(mínimo {cls.HEADER_SIZE})"
            )

        # 2. Desempacotar cabeçalho de 12 bytes
        byte0, byte1, seq_num, timestamp, ssrc = struct.unpack(
            "!BBHII", rtp_bytes[:cls.HEADER_SIZE]
        )

        # 3. Extrair campos do byte0
        pkt.version   = (byte0 >> 6) & 0x03
        pkt.padding   = (byte0 >> 5) & 0x01
        pkt.extension = (byte0 >> 4) & 0x01
        pkt.cc        = byte0 & 0x0F

        # 4. Extrair campos do byte1
        pkt.marker = (byte1 >> 7) & 0x01
        pkt.pt     = byte1 & 0x7F

        # 5. Campos de 16/32 bits
        pkt.seq_num   = seq_num
        pkt.timestamp = timestamp
        pkt.ssrc      = ssrc

        # 6. Payload = tudo após os 12 bytes de cabeçalho
        pkt.payload = rtp_bytes[cls.HEADER_SIZE:]

        return pkt

    # ──────────────────────────────────────────────────────────────────────────
    # Utilitários
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def current_timestamp_ms() -> int:
        """
        Retorna o timestamp atual em milissegundos (usando perf_counter
        para resolução sub-milissegundo sem deriva de relógio do sistema).
        """
        return int(time.perf_counter() * 1000) & 0xFFFFFFFF

    def __repr__(self) -> str:
        return (
            f"RtpPacket(stream_id={self.stream_id!r}, "
            f"seq={self.seq_num}, ts={self.timestamp}, "
            f"ssrc={self.ssrc}, payload={len(self.payload)}B)"
        )

