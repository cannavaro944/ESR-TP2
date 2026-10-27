import socket
import json

HOST = '0.0.0.0'
PORT = 9000

# Carregar o ficheiro de configuração do overlay
with open('topology.conf', 'r') as f:
    topology = json.load(f)

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((HOST, PORT))
server_socket.listen(5)

print(f"[*] Bootstrapper S1 a escutar na porta {PORT}...")

while True:
    client_socket, addr = server_socket.accept()
    try:
        data = client_socket.recv(1024).decode('utf-8')
        if data:
            node_id = data.strip()
            print(f"[*] Nó {node_id} registou-se a partir de {addr}")
            
            # Obter os vizinhos configurados para este nó
            neighbors = topology.get(node_id, [])
            
            # Enviar a lista de vizinhos em formato JSON
            response = json.dumps(neighbors)
            client_socket.sendall(response.encode('utf-8'))
    except Exception as e:
        print(f"[Erro] {e}")
    finally:
        client_socket.close()
