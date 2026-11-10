# ESR-TP2 — Streaming Multicast em Overlay (MEI - UMinho)

Projeto de Engenharia de Serviços em Rede (MEI — Universidade do Minho) que implementa um serviço de distribuição de vídeo em tempo real sobre uma rede Overlay com encaminhamento e replicação aplicacional (Multicast Overlay sobre UDP/RTP).

---

## 🏗️ Arquitetura do Sistema

O sistema divide-se em dois planos coordenados:

1. **Plano de Controlo (TCP / JSON)**:
   - `server.py`: Bootstrapper central (porta TCP 9000). Lê `topology.conf` e distribui a lista de vizinhos para cada nó.
   - `ott.py`: Daemon do nó overlay (Pessoas 1 e 2). Estabelece ligações TCP full-duplex com vizinhos, mede RTT/latência via PING/PONG, propaga anúncios de rota com métrica acumulada (`ANUNCIO`), e gere ativação (`ATIVA`) e desativação (`DESATIVA`) de rotas pelo caminho inverso.

2. **Plano de Dados (UDP / RTP — Porta 25000)** (Pessoa 3):
   - `rtp_packet.py`: Encapsulamento de frames JPEG em pacotes RTP padrão (RFC 3550, PT=26) com prefixo de stream: `<stream_id>|<cabeçalho_RTP_12B><payload_JPEG>`.
   - `video_stream.py`: Leitor sequencial de ficheiros `.Mjpeg` com suporte a looping infinito automático ao atingir o EOF.
   - `data_plane.py`:
     * `OverlayForwarder`: Escuta na porta UDP 25000, extrai o `stream_id` sem descodificar o payload RTP/JPEG, consulta os destinos ativos e replica os pacotes brutos.
     * `StreamServer`: Servidor de origem que mantém framerate estável (`time.perf_counter()` sem drift), compensando o tempo de I/O e rede. Só transmite se existirem destinos ativos na tabela de rotas.
   - `video_client.py`: Leitor de vídeo com interface gráfica (Tkinter + Pillow), desacoplamento por `queue.Queue` e encerramento limpo via protocolo `WM_DELETE_WINDOW`.

---

## 📁 Estrutura do Repositório

| Ficheiro | Descrição |
|---|---|
| `server.py` | Bootstrapper TCP (porta 9000) baseado em `topology.conf` |
| `topology.conf` | Ficheiro JSON de topologia e adjacências entre nós |
| `topology_tp2.xml` | Cenário do emulador de rede CORE |
| `ott.py` | Nó do Overlay (gestão de conexões TCP, rotas e plano de dados) |
| `rtp_packet.py` | Classe `RtpPacket` com codificação e descodificação rápida |
| `video_stream.py` | Classe `VideoStream` para leitura de ficheiros MJPEG em loop |
| `data_plane.py` | Classes `OverlayForwarder` e `StreamServer` |
| `video_client.py` | Interface gráfica do cliente de visualização de vídeo |
| `test_standalone.py` | Suite de testes unitários e de integração do plano de dados |
| `test_integration_ott.py` | Teste de integração entre `ott.py` e `data_plane.py` |

---

## 🚀 Como Executar

### Pré-requisitos
```bash
pip install Pillow
# Em Linux/CORE (para interface gráfica Tkinter):
sudo apt-get install python3-tk
```

### 1. Iniciar o Bootstrapper (no nó servidor central / S1)
```bash
python3 server.py
```

### 2. Iniciar um Nó Overlay (`ott.py`)
Em cada nó do CORE (ou terminal):
```bash
python3 ott.py <node_id> <bootstrapper_ip> <listen_port>
```
*Exemplo no nó S1:*
```bash
python3 ott.py S1 10.0.19.1 9001
```

### 3. Comandos Interativos do `ott.py`

No prompt `[<node_id>]>`:
- `vizinhos` — Lista vizinhos ativos e o atraso (RTT/2) medido via PING/PONG.
- `rotas` — Mostra a tabela de rotas, origem, métrica e destinos ativos.
- `anunciar <fluxo> [ficheiro.Mjpeg]` — Anuncia um fluxo e opcionalmente associa-o a um vídeo MJPEG local.
- `stream <fluxo> <ficheiro.Mjpeg> [fps]` — Carrega um vídeo e inicia o `StreamServer` (default 20 fps).
- `ativar <fluxo>` — Envia pedido `ATIVA` pelo caminho inverso para receber o fluxo.
- `desativar <fluxo>` — Envia pedido `DESATIVA` e cessa a receção do fluxo (pruning).
- `play <fluxo>` — Ativa a rota e abre a janela do `video_client.py`.
- `sair` — Encerra o nó, fecha sockets e conexões TCP de forma limpa.

### 4. Executar o Cliente de Vídeo Standalone
```bash
python3 video_client.py <stream_id>
```

---

## 🧪 Testes Automatizados

Executar a suite completa do Plano de Dados:
```bash
python test_standalone.py
```

Executar o teste de integração Controlo + Dados:
```bash
python test_integration_ott.py
```
