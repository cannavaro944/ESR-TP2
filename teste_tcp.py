import socket
import threading
import time

def inciar_servidor(host='127.0.0.1', port =5000):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	s.bind((host, port))
	s.listen()
	print(f"[Servidor] À escuta em {host}:{port}...")
	conn, addr = s.accept()
	with conn:
		print(f"[Servidor] Ligaçao aceite de {addr}")
		dados = conn.recv(1024)
		if dados:
			print(f"[Servidor] Recebeu: {dados.decode()}")
			conn.sendall(dados)

def iniciar_cliente(host='127.0.0.1', port=5000):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	s.connect((host, port))
	mensagem = "HELLO"
	print(f"[Cliente] A enviar: {mensagem}")
	s.sendall(mensagem.encode())
	dados = s.recv(1024)
	print(f"[Cliente] Eco recebido: {dados.decode()}")

threading.Thread(target=iniciar_servidor, daemon=True).start()

time.sleep(1)
iniciar_cliente()
