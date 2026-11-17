# Contexto Técnico — ESR TP2

## Arquitetura

O projeto implementa streaming multicast sobre uma rede overlay e separa a sinalização do transporte de vídeo:

- **Plano de controlo (TCP/JSON):** o bootstrapper distribui vizinhos e cada nó overlay mantém ligações TCP full-duplex. Estas ligações transportam `HELLO`, `PING`/`PONG`, `ANUNCIO`, `ATIVA`, `DESATIVA` e `TRACE`.
- **Plano de dados (UDP/RTP):** os frames MJPEG são encapsulados em RTP e enviados na porta UDP `25000`, com o formato `<stream_id>|<cabeçalho RTP de 12 bytes><payload JPEG>`. Os nós intermédios replicam os datagramas sem descodificar o JPEG.

## Estrutura do repositório

- `src/controlo/server.py` — bootstrapper TCP na porta 9000; lê `config/topology.conf`.
- `src/controlo/ott.py` — daemon do nó overlay: vizinhos, rotas e integração com o plano de dados.
- `src/dados/data_plane.py` — `OverlayForwarder` UDP e `StreamServer` de origem.
- `src/dados/rtp_packet.py` — criação e descodificação de pacotes RTP.
- `src/dados/video_stream.py` — leitor de ficheiros MJPEG com repetição automática no EOF.
- `src/dados/video_client.py` — cliente Tkinter/Pillow para reprodução do stream.
- `config/topology.conf` — topologia e adjacências em JSON.
- `topologias/topology_tp2.xml` — cenário do CORE.
- `assets/movie.Mjpeg` — vídeo de exemplo.
- `tests/` — testes standalone, de integração e de TCP experimental.
- `scripts/` — local reservado para automação de arranque do CORE/overlay.
- `ProgEx/` — material de apoio e exemplos originais, mantidos isolados do código do TP2.

## Estado de desenvolvimento

Está implementado o bootstrapper, o estabelecimento de ligações TCP entre vizinhos, a medição de atraso por `PING`/`PONG`, a inundação controlada e periódica de anúncios com métrica acumulada, e a ativação/desativação inversa de rotas. No plano de dados, está implementado o forwarder UDP/RTP que consulta os destinos ativos e replica datagramas, bem como o servidor de origem com controlo de framerate e o cliente gráfico.

## Execução rápida no CORE

1. Copie a estrutura do projeto para os nós necessários e abra o cenário `topologias/topology_tp2.xml` no CORE.
2. No nó bootstrapper, a partir da raiz do projeto, execute `python3 -m src.controlo.server`.
3. Em cada nó overlay, execute `python3 -m src.controlo.ott <node_id> <bootstrapper_ip> <porta_tcp>`. Exemplo: `python3 -m src.controlo.ott S1 10.0.19.1 9001`.
4. No nó de origem, use `stream <fluxo> assets/movie.Mjpeg [fps]`; nos consumidores, use `play <fluxo>` depois de a rota ser anunciada.

Para validação local, execute `python3 -m tests.test_integration_ott` e `python3 -m tests.test_standalone`. O cliente gráfico requer Pillow e, no Linux/CORE, normalmente `python3-tk`.
