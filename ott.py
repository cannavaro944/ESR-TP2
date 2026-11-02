import socket
import json
import sys
import threading
import time

rotas_lock = threading.Lock()
tabela_rotas = {}
vizinhos_ativos = {}
atraso_vizinhos = {}  # [Elemento 1] Guarda o atraso em ms medido via PING/PONG


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
    except Exception as e:
        print(f"[-] Erro com vizinho {ip_vizinho}: {e}")
    finally:
        if ip_vizinho in vizinhos_ativos:
            del vizinhos_ativos[ip_vizinho]
        conn.close()
        print(f"[-] Vizinho {ip_vizinho} desligou-se (falha/abandono detetado via TCP).")


def monitorizar_latencia_vizinhos():
    """[Elemento 1] Envia PING periódico com timestamp para medir atraso de cada link."""
    while True:
        time.sleep(5)
        ts_atual = time.time()
        for ip in list(vizinhos_ativos.keys()):
            enviar_para_vizinho({"tipo": "PING", "ts": ts_atual}, ip)


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
    """[Elemento 2] Atualiza tabela de rotas com base no atraso acumulado."""
    id_fluxo = msg["fluxo"]
    custo_link = atraso_vizinhos.get(ip_vizinho, 1.0)
    nova_metrica = round(msg["metrica"] + custo_link, 3)
    
    atualizou_rota = False
    with rotas_lock:
        if id_fluxo not in tabela_rotas or nova_metrica < tabela_rotas[id_fluxo]["metrica"]:
            destinos_atuais = tabela_rotas[id_fluxo]["destinos"] if id_fluxo in tabela_rotas else set()
            estado_atual = tabela_rotas[id_fluxo]["ativa"] if id_fluxo in tabela_rotas else False
            tabela_rotas[id_fluxo] = {
                "origem": ip_vizinho,
                "metrica": nova_metrica,
                "destinos": destinos_atuais,
                "ativa": estado_atual
            }
            atualizou_rota = True
            print(f"\n[*] Nova Rota: {id_fluxo} via {ip_vizinho} (Atraso: {nova_metrica} ms)")
            
    if atualizou_rota:
        msg_reenvio = {"tipo": "ANUNCIO", "fluxo": id_fluxo, "metrica": nova_metrica}
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
                print(f"\n[*] Rota {id_fluxo} ATIVADA. A pedir ao nó {rota['origem']}...")
                if rota["origem"] != "LOCAL":
                    enviar_para_vizinho({"tipo": "ATIVA", "fluxo": id_fluxo}, rota["origem"])


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
        
    node_id = sys.argv[1]
    boot_ip = sys.argv[2]
    boot_port = 9000
    listen_port = int(sys.argv[3])
    
    threading.Thread(target=handle_incoming_connections, args=(listen_port,), daemon=True).start()
    threading.Thread(target=monitorizar_latencia_vizinhos, daemon=True).start()
    time.sleep(0.5)

    print(f"[*] A contactar o bootstrapper em {boot_ip}:{boot_port}...")
    bs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    bs_socket.connect((boot_ip, boot_port))
    bs_socket.sendall(node_id.encode('utf-8'))
    
    response = bs_socket.recv(4096).decode('utf-8')
    bs_socket.close()
    
    neighbors = json.loads(response)
    print(f"[*] Vizinhos atribuídos para {node_id}: {neighbors}")
    
    conectar_aos_vizinhos(node_id, neighbors, listen_port)
    
    print("\nComandos: vizinhos | rotas | anunciar <fluxo> | ativar <fluxo> | sair")
    while True:
        try:
            cmd = input(f"[{node_id}]> ").strip().split()
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
                    print(f"Fluxo: {f} | Origem: {r['origem']} | Atraso: {r['metrica']} ms | Destinos: {list(r['destinos'])} | Ativa: {r['ativa']}")
        elif acao == "anunciar":
            fluxo = cmd[1] if len(cmd) > 1 else "Stream1"
            with rotas_lock:
                tabela_rotas[fluxo] = {"origem": "LOCAL", "metrica": 0.0, "destinos": set(), "ativa": False}
            reenviar_a_todos_exceto({"tipo": "ANUNCIO", "fluxo": fluxo, "metrica": 0.0}, None)
        elif acao == "ativar":
            fluxo = cmd[1] if len(cmd) > 1 else "Stream1"
            with rotas_lock:
                if fluxo in tabela_rotas:
                    tabela_rotas[fluxo]["ativa"] = True
                    enviar_para_vizinho({"tipo": "ATIVA", "fluxo": fluxo}, tabela_rotas[fluxo]["origem"])
        elif acao == "sair":
            break
