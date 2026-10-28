import socket
import json
import sys
import threading

rotas_lock = threading.Lock()
tabelas_rotas = {}
vizinhos_ativos = {}


def handle_incoming_connections(listen_port):
    """Atua como servidor para aceitar conexões TCP dos nós vizinhos."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', listen_port))
    s.listen(5)
    print(f"[*] À escuta de vizinhos na porta {listen_port}...")
    
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_peer, args=(conn, addr), daemon=True).start()

def handle_peer(conn, addr):
    """Gere a comunicação bidirecional com um vizinho."""
    print(f"[+] Conexão aceite de vizinho em {addr}")
    ip_vizinho = addr[0]
    vizinhos_ativos[ip_vizinho] = conn
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            mensagem = json.loads(data.decode('utf-8'))
            if mensagem.get("tipo") == "ANUNCIO":
                processar_anuncio(mensagem, ip_vizinho)
            elif mensagem.get("tipo") == "ATIVA":
                processar_ativacao(mensagem, ip_vizinho)
    except Exception as e:
       print(f"[-] Erro com vizinho {addr}: {e}")
    finally:
        if ip_vizinho in vizinhos_ativos:
           del vizinhos_ativos[ip_vizinho]
        conn.close()

def processar_anuncio(msg, ip_vizinho):
    id_fluxo = msg["fluxo"]
    nova_metrica = msg["metrica"] + 1 
    
    atualizou_rota = False
    with rotas_lock:
        if id_fluxo not in tabela_rotas or nova_metrica < tabela_rotas[id_fluxo]["metrica"]:
            tabela_rotas[id_fluxo] = {
                "origem": ip_vizinho,
                "metrica": nova_metrica,
                "destinos": set(),
                "ativa": False
            }
            atualizou_rota = True
            print(f"[*] Nova Rota: {id_fluxo} via {ip_vizinho} (Metrica: {nova_metrica})")
            
    # Se descobrimos um caminho mais curto, reencaminhar o anúncio aos outros vizinhos
    if atualizou_rota:
        msg_reenvio = {"tipo": "ANUNCIO", "fluxo": id_fluxo, "metrica": nova_metrica}
        reenviar_a_todos_exceto(msg_reenvio, ip_vizinho)

def processar_ativacao(msg, ip_vizinho):
    id_fluxo = msg["fluxo"]
    with rotas_lock:
        if id_fluxo in tabela_rotas:
            rota = tabela_rotas[id_fluxo]
            rota["destinos"].add(ip_vizinho)
            
            # Se a rota estava inativa, ativa e avisa o próximo salto (origem)
            if not rota["ativa"]:
                rota["ativa"] = True
                print(f"[*] Rota {id_fluxo} ATIVADA. A pedir ao nó {rota['origem']}...")
                enviar_para_vizinho({"tipo": "ATIVA", "fluxo": id_fluxo}, rota["origem"])

def reenviar_a_todos_exceto(msg, ip_ignorar):
    dados = json.dumps(msg).encode('utf-8')
    for ip, conn in list(vizinhos_ativos.items()):
        if ip != ip_ignorar:
            try:
                conn.sendall(dados)
            except:
                pass

def enviar_para_vizinho(msg, ip_destino):
    if ip_destino in vizinhos_ativos:
        dados = json.dumps(msg).encode('utf-8')
        try:
            vizinhos_ativos[ip_destino].sendall(dados)
        except:
            pass
	


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python ott.py <node_id> <bootstrapper_ip> <listen_port>")
        sys.exit(1)
        
    node_id = sys.argv[1]
    boot_ip = sys.argv[2]
    boot_port = 9000
    listen_port = int(sys.argv[3])
    
    # 1. Registar no Bootstrapper para obter os vizinhos
    print(f"[*] A contactar o bootstrapper em {boot_ip}:{boot_port}...")
    bs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    bs_socket.connect((boot_ip, boot_port))
    bs_socket.sendall(node_id.encode('utf-8'))
    
    response = bs_socket.recv(4096).decode('utf-8')
    bs_socket.close()
    
    neighbors = json.loads(response)
    print(f"[*] Vizinhos atribuídos para {node_id}: {neighbors}")
    
    # 2. Iniciar a thread de escuta para aceitar conexões dos vizinhos
    threading.Thread(target=handle_incoming_connections, args=(listen_port,), daemon=True).start()
    
    # Manter o programa ativo
    input("[*] Pressiona Enter para sair...\n")
