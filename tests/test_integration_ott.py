"""
test_integration_ott.py — Teste de integração entre ott.py e data_plane.py
========================================================================
Valida a integração entre a Tabela de Rotas do Plano de Controlo (ott.py)
e os módulos de transmissão/reencaminhamento do Plano de Dados (data_plane.py).
"""

import sys
import threading
import time
from src.controlo import ott
from src.dados.data_plane import OverlayForwarder, StreamServer

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


def _reset_ott_state(node_id="nC"):
    with ott.rotas_lock:
        ott.tabela_rotas.clear()
        ott.rotas_candidatas.clear()
        ott.fluxos_em_play.clear()
    ott.vizinhos_ativos.clear()
    ott.vizinhos_ids.clear()
    ott.atraso_vizinhos.clear()
    ott.fluxos_locais.clear()
    ott.meu_id = node_id


def test_rerouting_desativa_origem_antiga_e_failover():
    print("\n=== A testar re-roteamento por menor atraso + DESATIVA + failover ===")
    _reset_ott_state("nC")

    enviados = []
    orig_enviar = ott.enviar_para_vizinho
    orig_reenviar = ott.reenviar_a_todos_exceto
    ott.enviar_para_vizinho = lambda msg, ip: enviados.append((ip, msg))
    ott.reenviar_a_todos_exceto = lambda msg, ip_ign: None

    try:
        ip_a, ip_b, ip_d, ip_e, ip_cli1, ip_cli2 = (
            "10.0.1.1", "10.0.2.1", "10.0.3.1", "10.0.4.1", "10.0.18.20", "10.0.18.21"
        )
        for ip in (ip_a, ip_b, ip_d, ip_e, ip_cli1, ip_cli2):
            ott.vizinhos_ativos[ip] = object()
        ott.atraso_vizinhos[ip_a] = 1.0
        ott.atraso_vizinhos[ip_b] = 4.0
        ott.atraso_vizinhos[ip_d] = 10.0
        ott.atraso_vizinhos[ip_e] = 1.0

        # 1. Chega primeiro rota mais lenta via B (metrica 4 + 4 = 8ms)
        ott.processar_anuncio(
            {"tipo": "ANUNCIO", "servidor": "S1", "fluxo": "Stream1", "metrica": 4.0, "seq": 1, "caminho": ["S1", "nB"]},
            ip_b,
        )
        assert ott.tabela_rotas["Stream1"]["origem"] == ip_b
        assert ott.tabela_rotas["Stream1"]["metrica"] == 8.0

        # 2. Dois clientes ativam multicast neste nó
        ott.processar_ativacao({"tipo": "ATIVA", "fluxo": "Stream1"}, ip_cli1)
        ott.processar_ativacao({"tipo": "ATIVA", "fluxo": "Stream1"}, ip_cli2)
        dests = set(ott.obter_destinos_ativos("Stream1"))
        assert dests == {ip_cli1, ip_cli2}, f"Esperado multicast para 2 clientes, obtido {dests}"
        enviados.clear()

        # 3. Chegam rotas alternativas: D (20ms), A (2ms - mais rápida!), e E (4ms mas passa por nA!)
        ott.processar_anuncio(
            {"tipo": "ANUNCIO", "servidor": "S1", "fluxo": "Stream1", "metrica": 10.0, "seq": 1, "caminho": ["S1", "nD"]},
            ip_d,
        )
        assert ott.tabela_rotas["Stream1"]["origem"] == ip_b, "Rota mais lenta não deve substituir B"

        ott.processar_anuncio(
            {"tipo": "ANUNCIO", "servidor": "S1", "fluxo": "Stream1", "metrica": 1.0, "seq": 1, "caminho": ["S1", "nA"]},
            ip_a,
        )
        assert ott.tabela_rotas["Stream1"]["origem"] == ip_a, "Deve trocar para A (menor atraso: 2ms)"
        assert ott.tabela_rotas["Stream1"]["metrica"] == 2.0
        # Deve ativar A primeiro e DESATIVAR a origem antiga B (sem tráfego duplicado)
        assert enviados == [
            (ip_a, {"tipo": "ATIVA", "fluxo": "Stream1"}),
            (ip_b, {"tipo": "DESATIVA", "fluxo": "Stream1"}),
        ], f"Sequência inesperada de controlo ao trocar de rota: {enviados}"

        # Rota via E (4ms) passa por nA — entra em candidatas mas deve ser limpa se nA cair
        ott.processar_anuncio(
            {"tipo": "ANUNCIO", "servidor": "S1", "fluxo": "Stream1", "metrica": 3.0, "seq": 1, "caminho": ["S1", "nA", "nE"]},
            ip_e,
        )

        # Anúncio em ciclo (já contém nC) deve ser ignorado
        ott.processar_anuncio(
            {"tipo": "ANUNCIO", "servidor": "S1", "fluxo": "Stream1", "metrica": 0.1, "seq": 2, "caminho": ["S1", "nC", "nD"]},
            ip_d,
        )
        assert ott.tabela_rotas["Stream1"]["origem"] == ip_a, "Anúncio em ciclo deve ser ignorado"

        # 4. Simular falha do nó A (origem atual)
        enviados.clear()
        del ott.vizinhos_ativos[ip_a]
        ott.limpar_rotas_vizinho_falhado(ip_a, id_morto="nA")

        # Deve descartar E (que passava por nA) e fazer failover imediato para B (8ms < D 20ms)
        assert ott.tabela_rotas["Stream1"]["origem"] == ip_b, (
            f"Failover devia escolher B (8ms), escolheu {ott.tabela_rotas['Stream1']['origem']}"
        )
        assert ott.tabela_rotas["Stream1"]["metrica"] == 8.0
        assert ott.tabela_rotas["Stream1"]["ativa"] is True
        assert set(ott.obter_destinos_ativos("Stream1")) == {ip_cli1, ip_cli2}
        assert enviados == [
            (ip_b, {"tipo": "ATIVA", "fluxo": "Stream1"}),
        ], f"Failover devia enviar ATIVA imediato para B, enviou: {enviados}"

        print("[OK] Re-roteamento sem duplicados, prevenção de ciclos e failover por menor atraso validados!")
    finally:
        ott.enviar_para_vizinho = orig_enviar
        ott.reenviar_a_todos_exceto = orig_reenviar
        _reset_ott_state("")


if __name__ == "__main__":
    test_integration()
    test_rerouting_desativa_origem_antiga_e_failover()
