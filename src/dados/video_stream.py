"""
video_stream.py — Módulo de leitura de ficheiros MJPEG
======================================================
Lê sequencialmente frames JPEG de um ficheiro .Mjpeg com o formato:
    [tamanho 5 bytes ASCII][frame JPEG N bytes] ...

O ficheiro é percorrido em loop infinito (seek(0) ao chegar ao EOF),
garantindo transmissão contínua sem interrupção.

Formato de ficheiro:
    Cada frame é precedido por 5 bytes ASCII que representam o tamanho
    em bytes do frame JPEG a seguir, com zero-padding à esquerda.
    Exemplo: b"12345" → próximos 12345 bytes são o frame JPEG.

Utilizado por:
    - StreamServer (data_plane.py) para alimentar o pipeline de envio RTP

Autor: Plano de Dados — TP2 ESR/MEI-UMinho
"""

import logging
import os

logger = logging.getLogger(__name__)

# Tamanho fixo do campo de tamanho do frame (bytes ASCII)
FRAME_SIZE_FIELD = 5


class VideoStreamError(Exception):
    """Exceção base para erros de leitura de stream de vídeo."""
    pass


class VideoStream:
    """
    Leitor de ficheiros MJPEG com looping automático ao EOF.

    O formato do ficheiro é:
        [5 bytes ASCII com tamanho][N bytes JPEG][5 bytes ASCII com tamanho][N bytes JPEG]...

    Parâmetros
    ----------
    filename : str
        Caminho para o ficheiro .Mjpeg a abrir.

    Exemplo de uso
    --------------
    >>> vs = VideoStream("movie.Mjpeg")
    >>> frame = vs.next_frame()   # bytes JPEG do primeiro frame
    >>> vs.close()
    """

    def __init__(self, filename: str):
        """
        Abre o ficheiro MJPEG para leitura.

        Lança
        -----
        VideoStreamError — se o ficheiro não existir ou não puder ser aberto.
        """
        self.filename = filename

        if not os.path.isfile(filename):
            raise VideoStreamError(
                f"Ficheiro de vídeo não encontrado: {filename!r}"
            )

        try:
            # Abertura em modo binário; ficheiro mantido aberto durante toda a vida
            # do objeto para evitar overhead de open/close por frame
            self._file = open(filename, "rb")
        except OSError as exc:
            raise VideoStreamError(
                f"Não foi possível abrir {filename!r}: {exc}"
            ) from exc

        # Posição inicial após abertura (deve ser 0)
        self._frame_count = 0
        logger.info("VideoStream: ficheiro %r aberto com sucesso.", filename)

    # ──────────────────────────────────────────────────────────────────────────
    # Interface pública
    # ──────────────────────────────────────────────────────────────────────────

    def next_frame(self) -> bytes:
        """
        Lê e retorna o próximo frame JPEG do ficheiro.

        Comportamento ao EOF:
            Faz seek(0) e reinicia a leitura desde o início (looping infinito).

        Retorna
        -------
        bytes — dados brutos JPEG do frame.

        Lança
        -----
        VideoStreamError — se o ficheiro estiver corrompido (cabeçalho inválido
                           ou frame truncado).
        """
        # ── 1. Ler os 5 bytes de tamanho do frame ────────────────────────────
        size_field = self._file.read(FRAME_SIZE_FIELD)

        if len(size_field) < FRAME_SIZE_FIELD:
            # EOF (parcial ou total) → reiniciar ao início do ficheiro
            logger.debug(
                "VideoStream: EOF atingido após %d frames; a reiniciar.",
                self._frame_count
            )
            self._file.seek(0)
            size_field = self._file.read(FRAME_SIZE_FIELD)

            if len(size_field) < FRAME_SIZE_FIELD:
                raise VideoStreamError(
                    f"Ficheiro {self.filename!r} está vazio ou corrompido "
                    f"(não foi possível ler o cabeçalho de tamanho)."
                )

        # ── 2. Converter ASCII → inteiro ──────────────────────────────────────
        try:
            frame_size = int(size_field.decode("ascii").strip())
        except (ValueError, UnicodeDecodeError) as exc:
            raise VideoStreamError(
                f"Cabeçalho de tamanho inválido em {self.filename!r} "
                f"(posição {self._file.tell() - FRAME_SIZE_FIELD}): "
                f"{size_field!r} → {exc}"
            ) from exc

        if frame_size <= 0:
            raise VideoStreamError(
                f"Tamanho de frame inválido ({frame_size}) em {self.filename!r}."
            )

        # ── 3. Ler os N bytes do frame JPEG ───────────────────────────────────
        frame_data = self._file.read(frame_size)

        if len(frame_data) < frame_size:
            # Frame truncado → pode ser o último frame incompleto do ficheiro;
            # reinicia e tenta novamente uma única vez
            logger.warning(
                "VideoStream: frame truncado (esperado %d bytes, leu %d). "
                "A reiniciar o ficheiro.",
                frame_size, len(frame_data)
            )
            self._file.seek(0)
            return self.next_frame()  # Chamada recursiva única para recuperar

        self._frame_count += 1
        return frame_data

    @property
    def frame_count(self) -> int:
        """Número total de frames lidos desde a abertura (inclui loops)."""
        return self._frame_count

    def close(self):
        """Fecha o ficheiro e liberta recursos."""
        if self._file and not self._file.closed:
            self._file.close()
            logger.info(
                "VideoStream: ficheiro %r fechado após %d frames.",
                self.filename, self._frame_count
            )

    # ──────────────────────────────────────────────────────────────────────────
    # Context manager (with VideoStream(...) as vs:)
    # ──────────────────────────────────────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # Não suprime exceções

    def __repr__(self) -> str:
        return (
            f"VideoStream(filename={self.filename!r}, "
            f"frames_lidos={self._frame_count})"
        )

