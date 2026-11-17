import socket
import threading
import time


def inciar_servidor(host="127.0.0.1", port=5000):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, port))
        sock.listen()
        print(f"[Servidor] À escuta em {host}:{port}...")
        conn, addr = sock.accept()
        with conn:
            print(f"[Servidor] Ligação aceite de {addr}")
            dados = conn.recv(1024)
            if dados:
                print(f"[Servidor] Recebeu: {dados.decode()}")
                conn.sendall(dados)


def iniciar_cliente(host="127.0.0.1", port=5000):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        mensagem = "HELLO"
        print(f"[Cliente] A enviar: {mensagem}")
        sock.sendall(mensagem.encode())
        dados = sock.recv(1024)
        print(f"[Cliente] Eco recebido: {dados.decode()}")


if __name__ == "__main__":
    threading.Thread(target=inciar_servidor, daemon=True).start()
    time.sleep(1)
    iniciar_cliente()
