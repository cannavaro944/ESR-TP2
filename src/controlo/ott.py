import os
import socket
import json
import sys
import threading
import time
import subprocess

from src.dados.data_plane import OverlayForwarder, StreamServer

rotas_lock = threading.Lock()
tabela_rotas = {}
rotas_candidatas = {}     # Guarda rotas alternativas por fluxo/vizinho para failover rápido
vizinhos_ativos = {}
vizinhos_ids = {}         # Mapeia IP do vizinho -> node_id (aprendido via HELLO)
atraso_vizinhos = {}      # [Elemento 1] Guarda o atraso em ms medido via PING/PONG
fluxos_locais = {}        # Guarda os fluxos anunciados por este servidor (e o seu seq)
fluxos_em_play = set()    # [Novo] Guarda os fluxos que este próprio nó está a assistir
meu_id = ""


def log_seguro(texto):
    """Escreve um log de uma thread sem destruir o prompt interativo."""
    sys.stdout.write(f"\r\033[K{texto}\n[{meu_id}]> ")
    sys.stdout.flush()


def obter_destinos_ativos(fluxo):
    """
    [Elemento 2 e 3] Devolve lista de IPs (strings) para o Forwarder enviar os pacotes UDP.
    """
    destinos_finais = []
    with rotas_lock:
        if fluxo in tabela_rotas and tabela_rotas[fluxo].get("ativa", False):
            # 1. Adiciona os IPs dos vizinhos que pediram o vídeo
            for ip in tabela_rotas[fluxo].get("destinos", set()):
                destinos_finais.append(ip)

        # 2. Se este próprio nó estiver a assistir (play), adiciona o IP local
        if fluxo in fluxos_em_play:
            destinos_finais.append("127.0.0.1")

    return destinos_finais

def handle_incoming_connections(listen_port):
    """[Elemento 1] Servidor TCP para aceitar conexões dos vizinhos."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', listen_port))
    s.listen(5)
    print(f"[*] À escuta de vizinhos na porta {listen_port}...")
    
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_peer, args=(conn, addr, False), daemon=True).start()


def handle_peer(conn, addr, iniciador=False):
    """[Elemento 1] Gere a ligação TCP full-duplex, falhas e mensagens de controlo."""
    ip_vizinho = addr[0]
    vizinhos_ativos[ip_vizinho] = conn
    if ip_vizinho not in atraso_vizinhos:
        atraso_vizinhos[ip_vizinho] = 1.0
    print(f"[+] Vizinho ligado em TCP: {ip_vizinho}")
    if not iniciador and meu_id:
        try:
            conn.sendall((json.dumps({"tipo": "HELLO", "id": meu_id}) + "\n").encode('utf-8'))
        except Exception:
            pass
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            for linha in data.decode('utf-8').splitlines():
                if not linha.strip():
                    continue
                mensagem = json.loads(linha)
                tipo = mensagem.get("tipo")
                
                if tipo == "HELLO":
                    viz_id = mensagem.get("id")
                    if viz_id:
                        vizinhos_ids[ip_vizinho] = viz_id
                    print(f"[HELLO] Vizinho {viz_id} ({ip_vizinho}) ativo.")
                elif tipo == "PING":
                    enviar_para_vizinho({"tipo": "PONG", "ts": mensagem["ts"]}, ip_vizinho)
                elif tipo == "PONG":
                    rtt_ms = (time.time() - mensagem["ts"]) * 1000.0
                    atraso_vizinhos[ip_vizinho] = round(rtt_ms / 2.0, 3)
                elif tipo == "ANUNCIO":
                    processar_anuncio(mensagem, ip_vizinho)
                elif tipo == "ATIVA":
                    processar_ativacao(mensagem, ip_vizinho)
                elif tipo == "DESATIVA":
                    processar_desativacao(mensagem, ip_vizinho)
                elif tipo == "TRACE":
                    processar_trace(mensagem, ip_vizinho)
    except Exception as e:
        print(f"[-] Erro com vizinho {ip_vizinho}: {e}")
    finally:
        era_ligacao_ativa = vizinhos_ativos.get(ip_vizinho) is conn
        id_morto = None
        if era_ligacao_ativa:
            del vizinhos_ativos[ip_vizinho]
            atraso_vizinhos.pop(ip_vizinho, None)
            id_morto = vizinhos_ids.pop(ip_vizinho, None)
            limpar_rotas_vizinho_falhado(ip_vizinho, id_morto=id_morto)
        conn.close()
        if era_ligacao_ativa:
            print(f"[-] Vizinho {ip_vizinho} desligou-se (rotas associadas atualizadas).")


def limpar_rotas_vizinho_falhado(ip_morto, id_morto=None):
    """[Elementos 1 e 2] Remove vizinho caído e faz failover imediato para a melhor rota alternativa."""
    desativar_a_montante = []
    ativar_failover = []
    with rotas_lock:
        # Limpar o vizinho morto (e rotas que passem por ele) do cache de candidatas
        for id_fluxo, cands in list(rotas_candidatas.items()):
            cands.pop(ip_morto, None)
            if id_morto:
                for cand_ip in list(cands.keys()):
                    if id_morto in cands[cand_ip].get("caminho", []):
                        cands.pop(cand_ip, None)

        for id_fluxo, rota in list(tabela_rotas.items()):
            if ip_morto in rota["destinos"]:
                rota["destinos"].remove(ip_morto)
                print(f"[*] Vizinho caído {ip_morto} removido dos destinos do fluxo {id_fluxo}.")
                if not rota["destinos"] and id_fluxo not in fluxos_em_play and rota["origem"] != "LOCAL":
                    rota["ativa"] = False
                    if rota["origem"] != ip_morto:
                        desativar_a_montante.append((id_fluxo, rota["origem"]))

            if rota["origem"] == ip_morto:
                # Procurar a rota alternativa ativa com menor atraso acumulado
                cands = rotas_candidatas.get(id_fluxo, {})
                melhor_ip = None
                melhor_cand = None
                for cand_ip, cand in cands.items():
                    if cand_ip == ip_morto or cand_ip not in vizinhos_ativos:
                        continue
                    if cand_ip in rota["destinos"]:
                        continue
                    if melhor_cand is None or cand["metrica"] < melhor_cand["metrica"]:
                        melhor_ip = cand_ip
                        melhor_cand = cand

                if melhor_ip is not None:
                    rota["origem"] = melhor_ip
                    rota["metrica"] = melhor_cand["metrica"]
                    rota["servidor"] = melhor_cand["servidor"]
                    rota["seq"] = max(rota.get("seq", 0), melhor_cand.get("seq", 0))
                    log_seguro(
                        f"[FAILOVER] Origem ({ip_morto}) falhou! Rota '{id_fluxo}' comutada "
                        f"para {melhor_ip} (Atraso: {melhor_cand['metrica']} ms)."
                    )
                    if rota["ativa"]:
                        ativar_failover.append((id_fluxo, melhor_ip))
                else:
                    print(f"[!] A origem ({ip_morto}) do fluxo {id_fluxo} falhou! A aguardar rota alternativa...")
                    rota["metrica"] = float('inf')

    for id_fluxo, ip_novo in ativar_failover:
        enviar_para_vizinho({"tipo": "ATIVA", "fluxo": id_fluxo}, ip_novo)
    for id_fluxo, ip_orig in desativar_a_montante:
        enviar_para_vizinho({"tipo": "DESATIVA", "fluxo": id_fluxo}, ip_orig)


def monitorizar_latencia_vizinhos():
    """[Elemento 1] Envia PING periódico com timestamp para medir atraso de cada link."""
    while True:
        time.sleep(5)
        ts_atual = time.time()
        for ip in list(vizinhos_ativos.keys()):
            enviar_para_vizinho({"tipo": "PING", "ts": ts_atual}, ip)


def anuncios_periodicos_servidor():
    """[Elemento 2] Envia anúncios periódicos para todos os fluxos gerados localmente por este servidor."""
    while True:
        time.sleep(10)
        for fluxo in list(fluxos_locais.keys()):
            fluxos_locais[fluxo] += 1
            seq = fluxos_locais[fluxo]
            msg = {
                "tipo": "ANUNCIO",
                "servidor": meu_id,
                "fluxo": fluxo,
                "metrica": 0.0,
                "seq": seq,
                "caminho": [meu_id] if meu_id else [],
            }
            reenviar_a_todos_exceto(msg, None)


def conectar_aos_vizinhos(node_id, neighbors, listen_port):
    """[Elemento 1] Liga-se ativamente via TCP aos vizinhos devolvidos pelo Bootstrapper."""
    for ip_viz in neighbors:
        if ip_viz in vizinhos_ativos:
            continue
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect((ip_viz, listen_port))
            s.settimeout(None)
            s.sendall((json.dumps({"tipo": "HELLO", "id": node_id}) + "\n").encode('utf-8'))
            s.sendall((json.dumps({"tipo": "PING", "ts": time.time()}) + "\n").encode('utf-8'))
            threading.Thread(target=handle_peer, args=(s, (ip_viz, listen_port), True), daemon=True).start()
        except Exception:
            print(f"[*] Vizinho {ip_viz} ainda não iniciou (ligará quando arrancar).")


def processar_anuncio(msg, ip_vizinho):
    """[Elemento 2] Atualiza tabela de rotas com base no atraso acumulado, previne ciclos e desativa origem antiga."""
    id_fluxo = msg["fluxo"]
    servidor_id = msg.get("servidor", "S1")
    seq = msg.get("seq", 1)
    caminho_recebido = list(msg.get("caminho", []))

    # Prevenção de ciclos (path-vector): ignora anúncios que já passaram por este nó
    if meu_id and meu_id in caminho_recebido:
        return

    custo_link = atraso_vizinhos.get(ip_vizinho, 1.0)
    nova_metrica = round(msg["metrica"] + custo_link, 3)
    novo_caminho = caminho_recebido + ([meu_id] if meu_id else [])

    atualizou_rota = False
    re_ativar_nova_origem = False
    desativar_origem_antiga = None
    rota_descoberta = False
    origem_alterada = False

    with rotas_lock:
        rota_atual = tabela_rotas.get(id_fluxo)
        if rota_atual and rota_atual["origem"] == "LOCAL":
            return

        # Registar rota candidata deste vizinho para failover instantâneo se a origem atual cair
        candidatos = rotas_candidatas.setdefault(id_fluxo, {})
        candidatos[ip_vizinho] = {
            "servidor": servidor_id,
            "metrica": nova_metrica,
            "seq": seq,
            "caminho": caminho_recebido,
        }

        if (rota_atual is None or 
            seq > rota_atual.get("seq", 0) or 
            (seq == rota_atual.get("seq", 0) and nova_metrica < rota_atual["metrica"])):
            
            destinos_atuais = set(rota_atual["destinos"]) if rota_atual else set()
            estado_anterior = rota_atual["ativa"] if rota_atual else False
            origem_antiga = rota_atual["origem"] if rota_atual else None
            rota_descoberta = rota_atual is None
            origem_alterada = origem_antiga is not None and origem_antiga != ip_vizinho

            # Evitar ciclo de encaminhamento UDP se a nova origem estava antes nos destinos
            destinos_atuais.discard(ip_vizinho)
            estado_atual = estado_anterior and (bool(destinos_atuais) or id_fluxo in fluxos_em_play)

            tabela_rotas[id_fluxo] = {
                "servidor": servidor_id,
                "origem": ip_vizinho,
                "metrica": nova_metrica,
                "destinos": destinos_atuais,
                "ativa": estado_atual,
                "seq": seq
            }
            atualizou_rota = True
            
            if estado_atual and origem_alterada:
                re_ativar_nova_origem = True
            if estado_anterior and origem_alterada and origem_antiga != "LOCAL":
                desativar_origem_antiga = origem_antiga

            if rota_descoberta:
                log_seguro(
                    f"[*] Rota descoberta: {id_fluxo} via {ip_vizinho} "
                    f"(Atraso: {nova_metrica} ms | Ronda: {seq})"
                )
            elif origem_alterada:
                log_seguro(
                    f"[RE-ROTEAMENTO] Rota para '{id_fluxo}' transferida de "
                    f"{origem_antiga} para {ip_vizinho} (Atraso: {nova_metrica} ms)!"
                )
            
    # Make-before-break: ativa primeiro a nova origem e depois poda a origem antiga (evita duplicados)
    if re_ativar_nova_origem:
        enviar_para_vizinho({"tipo": "ATIVA", "fluxo": id_fluxo}, ip_vizinho)
    if desativar_origem_antiga:
        enviar_para_vizinho({"tipo": "DESATIVA", "fluxo": id_fluxo}, desativar_origem_antiga)

    if atualizou_rota:
        msg_reenvio = {
            "tipo": "ANUNCIO",
            "servidor": servidor_id,
            "fluxo": id_fluxo,
            "metrica": nova_metrica,
            "seq": seq,
            "caminho": novo_caminho,
        }
        reenviar_a_todos_exceto(msg_reenvio, ip_vizinho)


def processar_ativacao(msg, ip_vizinho):
    """[Elemento 2] Ativa a rota pelo caminho inverso."""
    id_fluxo = msg["fluxo"]
    with rotas_lock:
        if id_fluxo in tabela_rotas:
            rota = tabela_rotas[id_fluxo]
            rota["destinos"].add(ip_vizinho)
            if not rota["ativa"]:
                rota["ativa"] = True
                print(f"\n[*] Rota {id_fluxo} ATIVADA por {ip_vizinho}. A propagar para {rota['origem']}...")
                if rota["origem"] != "LOCAL":
                    enviar_para_vizinho({"tipo": "ATIVA", "fluxo": id_fluxo}, rota["origem"])


def processar_desativacao(msg, ip_vizinho):
    """[Elemento 2 & 3] Desativa a rota pelo caminho inverso (pruning multicast)."""
    id_fluxo = msg["fluxo"]
    with rotas_lock:
        if id_fluxo in tabela_rotas:
            rota = tabela_rotas[id_fluxo]
            if ip_vizinho in rota["destinos"]:
                rota["destinos"].discard(ip_vizinho)
            if not rota["destinos"] and id_fluxo not in fluxos_em_play:
                rota["ativa"] = False
                print(f"\n[*] Rota {id_fluxo} DESATIVADA (sem destinos restantes).")
                if rota["origem"] != "LOCAL":
                    enviar_para_vizinho({"tipo": "DESATIVA", "fluxo": id_fluxo}, rota["origem"])


def _aguardar_fecho_player(proc, id_fluxo, forwarder_ref=None):
    """[Elemento 3] Aguarda que a janela do VideoClient feche e envia DESATIVA se necessário."""
    proc.wait()
    enviar_desativa = None
    with rotas_lock:
        if id_fluxo in fluxos_em_play:
            fluxos_em_play.discard(id_fluxo)
            if id_fluxo in tabela_rotas:
                rota = tabela_rotas[id_fluxo]
                if not rota["destinos"]:
                    rota["ativa"] = False
                    log_seguro(f"[*] VideoClient fechado. Rota {id_fluxo} DESATIVADA (sem destinos restantes).")
                    if rota["origem"] != "LOCAL":
                        enviar_desativa = rota["origem"]
        if not fluxos_em_play and forwarder_ref is not None and not forwarder_ref._running:
            try:
                forwarder_ref.start()
            except OSError:
                pass
    if enviar_desativa:
        enviar_para_vizinho({"tipo": "DESATIVA", "fluxo": id_fluxo}, enviar_desativa)


def processar_trace(msg, ip_vizinho):
    """[Etapa 3] Encaminha mensagem de teste pelas rotas, regista log e acumula o caminho."""
    id_fluxo = msg["fluxo"]
    caminho = msg.get("caminho", []) + [meu_id]
    
    with rotas_lock:
        rota = tabela_rotas.get(id_fluxo)
        if not rota:
            print(f"[TRACE-LOG] Recebido TRACE para '{id_fluxo}' de {ip_vizinho}, mas sem rota conhecida.")
            return
        metrica_local = rota["metrica"]
        destinos = list(rota["destinos"])
        ativa = rota["ativa"]

    caminho_str = " -> ".join(caminho)
    print(f"\n[TRACE-LOG] Fluxo: {id_fluxo} | Caminho acumulado: {caminho_str} | Atraso até aqui: {metrica_local} ms | Rota Ativa: {ativa}")

    if destinos or id_fluxo in fluxos_em_play:
        msg_seguinte = {"tipo": "TRACE", "fluxo": id_fluxo, "caminho": caminho}
        for ip_dest in destinos:
            enviar_para_vizinho(msg_seguinte, ip_dest)
        if not destinos and id_fluxo in fluxos_em_play:
             print(f"[TRACE-FIM] O teste de percurso para '{id_fluxo}' chegou ao destino final ({meu_id})! Percurso: {caminho_str}")
    else:
        print(f"[TRACE-FIM] O teste de percurso para '{id_fluxo}' chegou ao destino final ({meu_id})! Percurso: {caminho_str}")


def reenviar_a_todos_exceto(msg, ip_ignorar):
    dados = (json.dumps(msg) + "\n").encode('utf-8')
    for ip, conn in list(vizinhos_ativos.items()):
        if ip != ip_ignorar:
            try:
                conn.sendall(dados)
            except:
                pass


def enviar_para_vizinho(msg, ip_destino):
    if ip_destino in vizinhos_ativos:
        dados = (json.dumps(msg) + "\n").encode('utf-8')
        try:
            vizinhos_ativos[ip_destino].sendall(dados)
        except:
            pass


def _iniciar_stream_local(stream_server, fluxo, ficheiro, fps=20.0):
    """[Elemento 2 & 3] Regista, inicia o StreamServer e anuncia um fluxo local."""
    fluxos_locais[fluxo] = fluxos_locais.get(fluxo, 0) + 1
    seq = fluxos_locais[fluxo]
    with rotas_lock:
        if fluxo not in tabela_rotas:
            tabela_rotas[fluxo] = {
                "servidor": meu_id,
                "origem": "LOCAL",
                "metrica": 0.0,
                "destinos": set(),
                "ativa": False,
                "seq": seq,
            }
    stream_server.start_stream(fluxo, ficheiro, fps=float(fps))
    print(f"[*] Stream {fluxo} pronto para transmissão a {fps} fps a partir de {ficheiro}.")
    reenviar_a_todos_exceto(
        {
            "tipo": "ANUNCIO",
            "servidor": meu_id,
            "fluxo": fluxo,
            "metrica": 0.0,
            "seq": seq,
            "caminho": [meu_id] if meu_id else [],
        },
        None,
    )


def _executar_play(fluxo, forwarder):
    """[Elemento 3] Ativa a rota e lança o cliente gráfico VideoClient na porta 25000."""
    with rotas_lock:
        fluxos_em_play.add(fluxo)
        if forwarder and forwarder._running:
            forwarder.stop()
        if fluxo in tabela_rotas:
            tabela_rotas[fluxo]["ativa"] = True
            if tabela_rotas[fluxo]["origem"] != "LOCAL":
                enviar_para_vizinho({"tipo": "ATIVA", "fluxo": fluxo}, tabela_rotas[fluxo]["origem"])
    print(f"[*] A abrir VideoClient local na porta 25000 para o fluxo {fluxo}...")
    env = os.environ.copy()
    if "DISPLAY" not in env:
        env["DISPLAY"] = ":0"
    proc = subprocess.Popen([sys.executable, "-m", "src.dados.video_client", fluxo], env=env)
    threading.Thread(target=_aguardar_fecho_player, args=(proc, fluxo, forwarder), daemon=True).start()


def _aguardar_rota_e_play(fluxo, forwarder, timeout_s=30.0):
    """Aguarda pelo anúncio de rota do fluxo e inicia play automaticamente (--play)."""
    limite = time.monotonic() + timeout_s
    while time.monotonic() < limite:
        with rotas_lock:
            existe = fluxo in tabela_rotas
        if existe:
            try:
                _executar_play(fluxo, forwarder)
            except Exception as exc:
                print(f"[-] Erro ao abrir o cliente de vídeo (--play {fluxo}): {exc}")
            return
        time.sleep(0.3)
    print(f"[-] Timeout ({timeout_s}s): rota para '{fluxo}' não descoberta para --play.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Nó Overlay (Controlo + Dados) — ESR TP2"
    )
    parser.add_argument("node_id", help="Identificador do nó (ex: S1, n10, n20)")
    parser.add_argument("bootstrapper_ip", help="IP do bootstrapper (porta 9000)")
    parser.add_argument("listen_port", type=int, help="Porta TCP para ligações entre vizinhos")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Mantém o nó ativo em background mesmo se stdin fechar (ideal para scripts no CORE)",
    )
    parser.add_argument(
        "--stream",
        action="append",
        nargs="+",
        metavar="ARG",
        help="Inicia automaticamente um stream: --stream <fluxo> <ficheiro.Mjpeg> [fps]",
    )
    parser.add_argument(
        "--play",
        action="append",
        metavar="FLUXO",
        help="Aguarda pela rota do fluxo e abre automaticamente o VideoClient",
    )

    args = parser.parse_args()
    meu_id = args.node_id
    boot_ip = args.bootstrapper_ip
    boot_port = 9000
    listen_port = args.listen_port

    threading.Thread(target=handle_incoming_connections, args=(listen_port,), daemon=True).start()
    threading.Thread(target=monitorizar_latencia_vizinhos, daemon=True).start()
    threading.Thread(target=anuncios_periodicos_servidor, daemon=True).start()
    time.sleep(0.5)

    print(f"[*] A contactar o bootstrapper em {boot_ip}:{boot_port}...")
    tentativas = 20 if args.daemon else 5
    response = None
    for tentativa in range(1, tentativas + 1):
        try:
            bs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            bs_socket.settimeout(3.0)
            bs_socket.connect((boot_ip, boot_port))
            bs_socket.sendall(meu_id.encode('utf-8'))
            response = bs_socket.recv(4096).decode('utf-8')
            bs_socket.close()
            break
        except OSError as exc:
            if tentativa == tentativas:
                print(f"[-] Falha ao contactar o bootstrapper em {boot_ip}:{boot_port}: {exc}")
                sys.exit(1)
            time.sleep(0.5)

    neighbors = json.loads(response)
    print(f"[*] Vizinhos atribuídos para {meu_id}: {neighbors}")

    conectar_aos_vizinhos(meu_id, neighbors, listen_port)

    # [Elemento 3 - Plano de Dados] Iniciar Forwarder UDP (porta 25000) e StreamServer
    try:
        forwarder = OverlayForwarder(obter_destinos_ativos)
        forwarder.start()
        print(f"[*] OverlayForwarder ativo na porta UDP 25000.")
    except Exception as e:
        print(f"[-] Aviso ao iniciar OverlayForwarder: {e}")
        forwarder = None

    stream_server = StreamServer(obter_destinos_ativos)
    stream_server.start()

    # Arranque automático de streams passados via --stream
    if args.stream:
        time.sleep(0.5)
        for item in args.stream:
            if len(item) < 2:
                print(f"[-] --stream inválido ({item}): uso --stream <fluxo> <ficheiro> [fps]")
                continue
            s_fluxo, s_fich = item[0], item[1]
            s_fps = float(item[2]) if len(item) > 2 else 20.0
            try:
                _iniciar_stream_local(stream_server, s_fluxo, s_fich, fps=s_fps)
            except Exception as e:
                print(f"[-] Erro ao iniciar --stream {s_fluxo}: {e}")

    # Arranque automático de clientes passados via --play
    if args.play:
        for p_fluxo in args.play:
            threading.Thread(
                target=_aguardar_rota_e_play,
                args=(p_fluxo, forwarder),
                daemon=True,
            ).start()

    print("\nComandos: vizinhos | rotas | anunciar <fluxo> [ficheiro] | stream <fluxo> <ficheiro> [fps] | ativar <fluxo> | desativar <fluxo> | play <fluxo> | trace <fluxo> | sair")
    while True:
        try:
            cmd = input(f"[{meu_id}]> ").strip().split()
        except EOFError:
            if args.daemon:
                try:
                    while True:
                        time.sleep(1.0)
                except KeyboardInterrupt:
                    pass
            break
        if not cmd:
            continue
        acao = cmd[0].lower()
        if acao == "vizinhos":
            for ip in list(vizinhos_ativos.keys()):
                print(f" -> {ip} (atraso: {atraso_vizinhos.get(ip, 1.0)} ms)")
        elif acao == "rotas":
            with rotas_lock:
                for f, r in tabela_rotas.items():
                    print(f"Fluxo: {f} (Srv: {r.get('servidor')}) | Origem: {r['origem']} | Atraso: {r['metrica']} ms | Destinos: {list(r['destinos'])} | Ativa: {r['ativa']}")
        elif acao == "anunciar":
            fluxo = cmd[1] if len(cmd) > 1 else "Stream1"
            ficheiro = cmd[2] if len(cmd) > 2 else None
            if ficheiro:
                try:
                    _iniciar_stream_local(stream_server, fluxo, ficheiro, fps=20.0)
                except Exception as e:
                    print(f"[-] Erro ao carregar vídeo para {fluxo}: {e}")
            else:
                fluxos_locais[fluxo] = fluxos_locais.get(fluxo, 0) + 1
                seq = fluxos_locais[fluxo]
                with rotas_lock:
                    tabela_rotas[fluxo] = {"servidor": meu_id, "origem": "LOCAL", "metrica": 0.0, "destinos": set(), "ativa": False, "seq": seq}
                reenviar_a_todos_exceto(
                    {
                        "tipo": "ANUNCIO",
                        "servidor": meu_id,
                        "fluxo": fluxo,
                        "metrica": 0.0,
                        "seq": seq,
                        "caminho": [meu_id] if meu_id else [],
                    },
                    None,
                )
                print(f"[*] Fluxo '{fluxo}' anunciado (e anúncios periódicos ativados a cada 10s).")
        elif acao == "stream":
            if len(cmd) < 3:
                print("Uso: stream <fluxo> <ficheiro_mjpeg> [fps]")
            else:
                fluxo = cmd[1]
                ficheiro = cmd[2]
                fps = float(cmd[3]) if len(cmd) > 3 else 20.0
                try:
                    _iniciar_stream_local(stream_server, fluxo, ficheiro, fps=fps)
                except Exception as e:
                    print(f"[-] Erro ao iniciar stream {fluxo}: {e}")
        elif acao == "ativar":
            fluxo = cmd[1] if len(cmd) > 1 else "Stream1"
            with rotas_lock:
                if fluxo in tabela_rotas:
                    tabela_rotas[fluxo]["ativa"] = True
                    enviar_para_vizinho({"tipo": "ATIVA", "fluxo": fluxo}, tabela_rotas[fluxo]["origem"])
                    print(f"[*] Pedido de ATIVA para {fluxo} enviado a {tabela_rotas[fluxo]['origem']}.")
        elif acao == "desativar":
            fluxo = cmd[1] if len(cmd) > 1 else "Stream1"
            enviar_desativa = None
            with rotas_lock:
                fluxos_em_play.discard(fluxo)  # Remove da reprodução local, se estivesse
                if fluxo in tabela_rotas:
                    rota = tabela_rotas[fluxo]
                    if not rota["destinos"]:
                        rota["ativa"] = False
                        if rota["origem"] != "LOCAL":
                            enviar_desativa = rota["origem"]
                        print(f"[*] Pedido de DESATIVA para {fluxo} enviado a {rota['origem']}.")
                    else:
                        print(f"[*] Consumo local de {fluxo} desativado (rota mantida para {len(rota['destinos'])} destino(s)).")
                if not fluxos_em_play and forwarder and not forwarder._running:
                    try:
                        forwarder.start()
                    except OSError:
                        pass
            if enviar_desativa:
                enviar_para_vizinho({"tipo": "DESATIVA", "fluxo": fluxo}, enviar_desativa)
        elif acao == "play":
            fluxo = cmd[1] if len(cmd) > 1 else "Stream1"
            try:
                _executar_play(fluxo, forwarder)
            except Exception as e:
                print(f"[-] Erro ao abrir o cliente de vídeo: {e}")
        elif acao == "trace":
            fluxo = cmd[1] if len(cmd) > 1 else "Stream1"
            with rotas_lock:
                rota = tabela_rotas.get(fluxo)
                destinos = list(rota["destinos"]) if rota else []
            if destinos or fluxo in fluxos_em_play:
                for ip_dest in destinos:
                    enviar_para_vizinho({"tipo": "TRACE", "fluxo": fluxo, "caminho": [meu_id]}, ip_dest)
                print(f"[*] Mensagem TRACE de teste de percurso enviada para os destinos de '{fluxo}'.")
            else:
                print(f"[-] Nenhum destino ativo para '{fluxo}'. Ativa primeiro a rota num cliente!")
        elif acao == "sair":
            if forwarder:
                forwarder.stop()
            stream_server.stop_all()
            break
