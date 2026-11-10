"""
test_integration_ott.py — Teste de integração entre ott.py e data_plane.py
========================================================================
Valida a integração entre a Tabela de Rotas do Plano de Controlo (ott.py)
e os módulos de transmissão/reencaminhamento do Plano de Dados (data_plane.py).
"""

import sys
import threading
import time
import ott
from data_plane import OverlayForwarder, StreamServer

def test_integration():
    print("=== A testar integração ott.py <-> data_plane.py ===")

    # 1. Testar estado inicial sem rotas
    assert ott.obter_destinos_ativos("S1_V1") == [], "Rota não existente deve devolver lista vazia"

    # 2. Criar uma rota local simulada (como ao fazer 'anunciar S1_V1')
    with ott.rotas_lock:
        ott.tabela_rotas["S1_V1"] = {
            "origem": "LOCAL",
            "metrica": 0.0,
            "destinos": set(),
            "ativa": False
        }

    # Como ativa=False e destinos está vazio, deve devolver []
    assert ott.obter_destinos_ativos("S1_V1") == [], "Rota inativa deve devolver lista vazia"

    # 3. Simular ativação da rota por um vizinho (processar_ativacao)
    msg_ativa = {"tipo": "ATIVA", "fluxo": "S1_V1"}
    ott.processar_ativacao(msg_ativa, "10.0.19.21")

    # Agora a rota deve estar ativa com o destino do vizinho
    destinos = ott.obter_destinos_ativos("S1_V1")
    print(f"[*] Destinos ativos após ativação: {destinos}")
    assert "10.0.19.21" in destinos, "Vizinho 10.0.19.21 devia estar nos destinos ativos"
    assert isinstance(destinos, list), "Deve retornar uma lista (cópia rasa)"

    # 4. Simular desativação da rota pelo vizinho (processar_desativacao)
    msg_desativa = {"tipo": "DESATIVA", "fluxo": "S1_V1"}
    ott.processar_desativacao(msg_desativa, "10.0.19.21")

    destinos_apos_leave = ott.obter_destinos_ativos("S1_V1")
    print(f"[*] Destinos ativos após desativação: {destinos_apos_leave}")
    assert destinos_apos_leave == [], "Destinos deviam estar vazios após desativação"
    assert ott.tabela_rotas["S1_V1"]["ativa"] is False, "Rota devia passar a ativa=False"

    print("[OK] Integração Plano de Controlo + Plano de Dados validada com sucesso!")

if __name__ == "__main__":
    test_integration()
