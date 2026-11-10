import socket
import json
import sys
import threading
import time
import subprocess

from data_plane import OverlayForwarder, StreamServer

rotas_lock = threading.Lock()
tabela_rotas = {}
vizinhos_ativos = {}
atraso_vizinhos = {}      # [Elemento 1] Guarda o atraso em ms medido via PING/PONG
fluxos_locais = {}        # Guarda os fluxos anunciados por este servidor (e o seu seq)
meu_id = ""


def obter_destinos_ativos(fluxo):
    """[Elemento 3 - Plano de Dados] Devolve cópia rasa dos IPs de destinos ativos para o fluxo."""
    with rotas_lock:
        if fluxo in tabela_rotas and tabela_rotas[fluxo].get("ativa", False):
            return list(tabela_rotas[fluxo].get("destinos", set()))
        return []


def handle_incoming_connections(listen_port):
    """[Elemento 1] Servidor TCP para aceitar conexões dos vizinhos."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', listen_port))
    s.listen(5)
    print(f"[*] À escuta de vizinhos na porta {listen_port}...")
    
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_peer, args=(conn, addr), daemon=True).start()


def handle_peer(conn, addr):
    """[Elemento 1] Gere a ligação TCP full-duplex, falhas e mensagens de controlo."""
    ip_vizinho = addr[0]
    vizinhos_ativos[ip_vizinho] = conn
    if ip_vizinho not in atraso_vizinhos:
        atraso_vizinhos[ip_vizinho] = 1.0
    print(f"[+] Vizinho ligado em TCP: {ip_vizinho}")
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
                    print(f"[HELLO] Vizinho {mensagem.get('id')} ({ip_vizinho}) ativo.")
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
        if ip_vizinho in vizinhos_ativos:
            del vizinhos_ativos[ip_vizinho]
        if ip_vizinho in atraso_vizinhos:
            del atraso_vizinhos[ip_vizinho]
        limpar_rotas_vizinho_falhado(ip_vizinho)
        conn.close()
        print(f"[-] Vizinho {ip_vizinho} desligou-se (rotas associadas atualizadas).")


def limpar_rotas_vizinho_falhado(ip_morto):
    """[Elementos 1 e 2] Remove o vizinho caído dos destinos e invalida rotas onde ele era a origem."""
    with rotas_lock:
        for id_fluxo, rota in list(tabela_rotas.items()):
            if ip_morto in rota["destinos"]:
                rota["destinos"].remove(ip_morto)
                print(f"[*] Vizinho caído {ip_morto} removido dos destinos do fluxo {id_fluxo}.")
                if not rota["destinos"] and rota["origem"] != "LOCAL":
                    rota["ativa"] = False
            if rota["origem"] == ip_morto:
                print(f"[!] A origem ({ip_morto}) do fluxo {id_fluxo} falhou! A aguardar rota alternativa...")
                rota["metrica"] = float('inf')


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
            msg = {"tipo": "ANUNCIO", "servidor": meu_id, "fluxo": fluxo, "metrica": 0.0, "seq": seq}
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
            threading.Thread(target=handle_peer, args=(s, (ip_viz, listen_port)), daemon=True).start()
        except Exception:
            print(f"[*] Vizinho {ip_viz} ainda não iniciou (ligará quando arrancar).")


def processar_anuncio(msg, ip_vizinho):
    """[Elemento 2] Atualiza tabela de rotas com base no atraso acumulado e número de sequência."""
    id_fluxo = msg["fluxo"]
    servidor_id = msg.get("servidor", "S1")
    seq = msg.get("seq", 1)
    custo_link = atraso_vizinhos.get(ip_vizinho, 1.0)
    nova_metrica = round(msg["metrica"] + custo_link, 3)
    
    atualizou_rota = False
    re_ativar_nova_origem = False

    with rotas_lock:
        rota_atual = tabela_rotas.get(id_fluxo)
        if rota_atual and rota_atual["origem"] == "LOCAL":
            return

        # Aceita se for fluxo novo, se for uma ronda de anúncio mais recente (seq maior), ou se for caminho mais rápido na mesma ronda
        if (rota_atual is None or 
            seq > rota_atual.get("seq", 0) or 
            (seq == rota_atual.get("seq", 0) and nova_metrica < rota_atual["metrica"])):
            
            destinos_atuais = rota_atual["destinos"] if rota_atual else set()
            estado_atual = rota_atual["ativa"] if rota_atual else False
            origem_antiga = rota_atual["origem"] if rota_atual else None

            tabela_rotas[id_fluxo] = {
                "servidor": servidor_id,
                "origem": ip_vizinho,
                "metrica": nova_metrica,
                "destinos": destinos_atuais,
                "ativa": estado_atual,
                "seq": seq
            }
            atualizou_rota = True
            
            # Se a rota já estava ativa mas a origem mudou (ex: falha do nó anterior), envia ATIVA para a nova origem
            if estado_atual and origem_antiga != ip_vizinho:
                re_ativar_nova_origem = True
                print(f"[RE-ROTEAMENTO] Rota para '{id_fluxo}' transferida de {origem_antiga} para {ip_vizinho}!")

            print(f"[*] Rota Atualizada: {id_fluxo} via {ip_vizinho} (Atraso: {nova_metrica} ms | Ronda: {seq})")
            
    if re_ativar_nova_origem:
        enviar_para_vizinho({"tipo": "ATIVA", "fluxo": id_fluxo}, ip_vizinho)

    if atualizou_rota:
        msg_reenvio = {"tipo": "ANUNCIO", "servidor": servidor_id, "fluxo": id_fluxo, "metrica": nova_metrica, "seq": seq}
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
            if not rota["destinos"]:
                rota["ativa"] = False
                print(f"\n[*] Rota {id_fluxo} DESATIVADA (sem destinos restantes).")
                if rota["origem"] != "LOCAL":
                    enviar_para_vizinho({"tipo": "DESATIVA", "fluxo": id_fluxo}, rota["origem"])


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

    if destinos:
        msg_seguinte = {"tipo": "TRACE", "fluxo": id_fluxo, "caminho": caminho}
        for ip_dest in destinos:
            enviar_para_vizinho(msg_seguinte, ip_dest)
    else:
        print(f"[TRACE-FIM] O teste de percurso para '{id_fluxo}' chegou ao destino final ({meu_id})! Percurso completo: {caminho_str}")


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


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python3 ott.py <node_id> <bootstrapper_ip> <listen_port>")
        sys.exit(1)
        
    meu_id = sys.argv[1]
    boot_ip = sys.argv[2]
    boot_port = 9000
    listen_port = int(sys.argv[3])
    
    threading.Thread(target=handle_incoming_connections, args=(listen_port,), daemon=True).start()
    threading.Thread(target=monitorizar_latencia_vizinhos, daemon=True).start()
    threading.Thread(target=anuncios_periodicos_servidor, daemon=True).start()
    time.sleep(0.5)

    print(f"[*] A contactar o bootstrapper em {boot_ip}:{boot_port}...")
    bs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    bs_socket.connect((boot_ip, boot_port))
    bs_socket.sendall(meu_id.encode('utf-8'))
    
    response = bs_socket.recv(4096).decode('utf-8')
    bs_socket.close()
    
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

    print("\nComandos: vizinhos | rotas | anunciar <fluxo> [ficheiro] | stream <fluxo> <ficheiro> [fps] | ativar <fluxo> | desativar <fluxo> | play <fluxo> | trace <fluxo> | sair")
    while True:
        try:
            cmd = input(f"[{meu_id}]> ").strip().split()
        except EOFError:
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
            fluxos_locais[fluxo] = fluxos_locais.get(fluxo, 0) + 1
            seq = fluxos_locais[fluxo]
            with rotas_lock:
                tabela_rotas[fluxo] = {"servidor": meu_id, "origem": "LOCAL", "metrica": 0.0, "destinos": set(), "ativa": False, "seq": seq}
            if ficheiro:
                try:
                    stream_server.start_stream(fluxo, ficheiro)
                    print(f"[*] StreamServer associado ao ficheiro {ficheiro} para o fluxo {fluxo}.")
                except Exception as e:
                    print(f"[-] Erro ao carregar vídeo para {fluxo}: {e}")
            reenviar_a_todos_exceto({"tipo": "ANUNCIO", "servidor": meu_id, "fluxo": fluxo, "metrica": 0.0, "seq": seq}, None)
            print(f"[*] Fluxo '{fluxo}' anunciado (e anúncios periódicos ativados a cada 10s).")
        elif acao == "stream":
            if len(cmd) < 3:
                print("Uso: stream <fluxo> <ficheiro_mjpeg> [fps]")
            else:
                fluxo = cmd[1]
                ficheiro = cmd[2]
                fps = float(cmd[3]) if len(cmd) > 3 else 20.0
                fluxos_locais[fluxo] = fluxos_locais.get(fluxo, 0) + 1
                seq = fluxos_locais[fluxo]
                with rotas_lock:
                    if fluxo not in tabela_rotas:
                        tabela_rotas[fluxo] = {"servidor": meu_id, "origem": "LOCAL", "metrica": 0.0, "destinos": set(), "ativa": False, "seq": seq}
                try:
                    stream_server.start_stream(fluxo, ficheiro, fps=fps)
                    print(f"[*] Stream {fluxo} pronto para transmissão a {fps} fps a partir de {ficheiro}.")
                    reenviar_a_todos_exceto({"tipo": "ANUNCIO", "servidor": meu_id, "fluxo": fluxo, "metrica": 0.0, "seq": seq}, None)
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
            with rotas_lock:
                if fluxo in tabela_rotas:
                    rota = tabela_rotas[fluxo]
                    rota["ativa"] = False
                    rota["destinos"].clear()
                    if rota["origem"] != "LOCAL":
                        enviar_para_vizinho({"tipo": "DESATIVA", "fluxo": fluxo}, rota["origem"])
                    print(f"[*] Pedido de DESATIVA para {fluxo} enviado a {rota['origem']}.")
        elif acao == "play":
            fluxo = cmd[1] if len(cmd) > 1 else "Stream1"
            # Primeiro ativa o fluxo para que os dados cheguem a este nó
            with rotas_lock:
                if fluxo in tabela_rotas:
                    tabela_rotas[fluxo]["ativa"] = True
                    if tabela_rotas[fluxo]["origem"] != "LOCAL":
                        enviar_para_vizinho({"tipo": "ATIVA", "fluxo": fluxo}, tabela_rotas[fluxo]["origem"])
            print(f"[*] A abrir VideoClient para o fluxo {fluxo}...")
            try:
                subprocess.Popen([sys.executable, "video_client.py", fluxo])
            except Exception as e:
                print(f"[-] Erro ao abrir video_client.py: {e}")
        elif acao == "trace":
            fluxo = cmd[1] if len(cmd) > 1 else "Stream1"
            with rotas_lock:
                rota = tabela_rotas.get(fluxo)
                destinos = list(rota["destinos"]) if rota else []
            if destinos:
                for ip_dest in destinos:
                    enviar_para_vizinho({"tipo": "TRACE", "fluxo": fluxo, "caminho": [meu_id]}, ip_dest)
                print(f"[*] Mensagem TRACE de teste de percurso enviada para os destinos ativos de '{fluxo}'.")
            else:
                print(f"[-] Nenhum destino ativo para '{fluxo}'. Ativa primeiro a rota num cliente!")
        elif acao == "sair":
            if forwarder:
                forwarder.stop()
            stream_server.stop_all()
            break
