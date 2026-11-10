"""
test_integration_ott.py — Teste de integração entre ott.py, data_plane.py e video_client.py
===========================================================================================
Valida:
  1. Formato (IP, Porta) em obter_destinos_ativos() (vizinhos na 25000 e player local na 25001)
  2. Coexistência na mesma máquina entre OverlayForwarder (25000) e cliente local (25001)
  3. Ativação e desativação (pruning) sem cortar vizinhos a jusante
  4. Aceitação do argumento posicional de porta no video_client.py
"""

import os
import socket
import subprocess
import sys
import tempfile
import time
import ott
from data_plane import OverlayForwarder, StreamServer, _normalize_dest
from rtp_packet import RtpPacket


def test_integration():
    print("=== A testar integração ott.py <-> data_plane.py <-> video_client.py ===")

    # 1. Testar _normalize_dest com ambos os formatos (string IP e tuplo (IP, porta))
    assert _normalize_dest("10.0.19.21") == ("10.0.19.21", 25000)
    assert _normalize_dest(("127.0.0.1", 25001)) == ("127.0.0.1", 25001)
    print("[OK] 1. Normalização de destinos (str e tuplo) validada.")

    # 2. Testar estado inicial sem rotas
    ott.tabela_rotas.clear()
    ott.fluxos_em_play.clear()
    assert ott.obter_destinos_ativos("S1_V1") == [], "Rota não existente deve devolver lista vazia"

    # 3. Criar uma rota local simulada (como ao fazer 'anunciar S1_V1')
    with ott.rotas_lock:
        ott.tabela_rotas["S1_V1"] = {
            "origem": "LOCAL",
            "metrica": 0.0,
            "destinos": set(),
            "ativa": False
        }
    assert ott.obter_destinos_ativos("S1_V1") == [], "Rota inativa deve devolver lista vazia"

    # 4. Simular ativação da rota por um vizinho (processar_ativacao)
    ott.processar_ativacao({"tipo": "ATIVA", "fluxo": "S1_V1"}, "10.0.19.21")
    destinos = ott.obter_destinos_ativos("S1_V1")
    print(f"[*] Destinos ativos após ATIVA do vizinho: {destinos}")
    assert ("10.0.19.21", 25000) in destinos, "Vizinho ('10.0.19.21', 25000) devia estar nos destinos"

    # 5. Simular 'play S1_V1' neste próprio nó ao mesmo tempo
    with ott.rotas_lock:
        ott.fluxos_em_play.add("S1_V1")
    destinos_com_play = ott.obter_destinos_ativos("S1_V1")
    print(f"[*] Destinos ativos com vizinho + play local: {destinos_com_play}")
    assert ("10.0.19.21", 25000) in destinos_com_play
    assert ("127.0.0.1", 25001) in destinos_com_play

    # 6. Simular DESATIVA do vizinho enquanto o play local continua ativo
    ott.processar_desativacao({"tipo": "DESATIVA", "fluxo": "S1_V1"}, "10.0.19.21")
    assert ott.tabela_rotas["S1_V1"]["ativa"] is True, "Rota deve manter-se ativa enquanto play local corre"
    assert ott.obter_destinos_ativos("S1_V1") == [("127.0.0.1", 25001)]

    # 7. Parar também o play local e confirmar que a rota desativa completamente
    with ott.rotas_lock:
        ott.fluxos_em_play.discard("S1_V1")
    ott.processar_desativacao({"tipo": "DESATIVA", "fluxo": "S1_V1"}, "10.0.19.21")
    assert ott.tabela_rotas["S1_V1"]["ativa"] is False, "Rota devia passar a ativa=False"
    assert ott.obter_destinos_ativos("S1_V1") == []
    print("[OK] 2. Lógica de ativação, play local (25001) e pruning validada.")

    # 8. Testar coexistência real de sockets: OverlayForwarder (25000) -> Player Local (25001)
    with ott.rotas_lock:
        ott.tabela_rotas["S1_V1"]["ativa"] = True
        ott.fluxos_em_play.add("S1_V1")

    player_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    player_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    player_sock.settimeout(2.0)
    player_sock.bind(("127.0.0.1", 25001))

    forwarder = OverlayForwarder(ott.obter_destinos_ativos)
    forwarder.start()

    try:
        # Enviar pacote RTP para o OverlayForwarder na porta 25000
        pkt = RtpPacket().make(seq_num=7, timestamp=1000, ssrc=99, payload=b"\xff\xd8\xff\xd9", stream_id="S1_V1")
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender.sendto(pkt.get_packet(), ("127.0.0.1", 25000))
        sender.close()

        # O player na porta 25001 deve receber o pacote reencaminhado pelo OverlayForwarder!
        recebido, _ = player_sock.recvfrom(65536)
        pkt_dec = RtpPacket.decode(recebido)
        assert pkt_dec.stream_id == "S1_V1" and pkt_dec.seq_num == 7
        print("[OK] 3. Coexistência sem conflito de portas (Forwarder:25000 -> Cliente:25001) comprovada!")
    finally:
        forwarder.stop()
        player_sock.close()
        ott.fluxos_em_play.clear()

    # 9. Testar se video_client.py aceita o argumento posicional de porta 25001
    res = subprocess.run([sys.executable, "video_client.py", "--help"], capture_output=True, text=True)
    assert res.returncode == 0 and "port" in res.stdout
    print("[OK] 4. Argumento posicional de porta em video_client.py validado.")

    print("\n[SUCESSO] Todos os testes de integração passaram a 100%!")


if __name__ == "__main__":
    test_integration()
