# Contexto Técnico — ESR TP2

## Arquitetura

O projeto implementa streaming multicast aplicacional sobre uma rede overlay e separa explicitamente a sinalização do transporte de vídeo:

- **Plano de controlo (TCP / JSON — portas `9000` e `9001`):** o bootstrapper (`server.py`, Estratégia 2 da Etapa 1) distribui a lista de vizinhos a cada nó. Os nós overlay (`ott.py`) mantêm ligações TCP full-duplex entre vizinhos com `HELLO` simétrico, deduplicação determinística por `node_id` em links virtuais multi-hop (`_registar_hello_vizinho`), medição periódica de latência (`PING`/`PONG` a cada 5s, `RTT / 2`), inundação controlada por iniciativa do servidor (Estratégia 1 da Etapa 2) com mensagens `ANUNCIO` periódicas (10s) contendo métrica acumulada e vetor de caminho (`caminho`) anti-ciclos, ativação inversa (`ATIVA`), pruning seletivo (`DESATIVA`) e teste de percurso (`TRACE`, Etapa 3).
- **Tolerância a falhas e otimização de rotas:** cada nó guarda rotas candidatas (`rotas_candidatas`) por vizinho. Ao descobrir uma rota mais rápida, aplica *make-before-break* (envia `ATIVA` à nova origem e `DESATIVA` à origem antiga, evitando vídeo duplicado). Se a origem atual cair (quebra TCP), `limpar_rotas_vizinho_falhado` purga o nó morto e qualquer candidata que passe por ele (`caminho`) e comuta imediatamente para a rota alternativa viva com menor atraso acumulado.
- **Plano de dados (UDP / RTP — porta `25000`):** frames MJPEG são encapsulados em RTP RFC 3550 (12 bytes, `V=2`, `PT=26`) com o formato no fio `<stream_id>|<cabeçalho RTP 12B><payload JPEG>` (Estratégia 1 da Etapa 4). Os nós intermédios replicam datagramas em bruto (`split(b"|", 1)`) sem descodificar RTP nem JPEG (*zero-decode*).

## Estrutura do repositório

- `src/controlo/server.py` — bootstrapper TCP na porta `9000`; lê `config/topology.conf`.
- `src/controlo/ott.py` — daemon do nó overlay: vizinhos TCP com deduplicação multi-hop, monitorização de latência, rotas com *path-vector* e failover imediato, CLI interativa e integração com o plano de dados (`obter_destinos_ativos`).
- `src/dados/rtp_packet.py` — codificação e descodificação de cabeçalhos RTP de 12 bytes (`RtpPacket`).
- `src/dados/video_stream.py` — leitor sequencial de ficheiros `.Mjpeg` com repetição automática em loop no EOF (`VideoStream`).
- `src/dados/data_plane.py` — `OverlayForwarder` (encaminhamento multicast UDP *zero-decode*) e `StreamServer` (multi-stream com controlo de framerate anti-drift via `time.perf_counter()`).
- `src/dados/video_client.py` — cliente gráfico Tkinter/Pillow desacoplado via `queue.Queue(maxsize=5)` e fecho limpo (`WM_DELETE_WINDOW` → `DESATIVA` automático).
- `config/topology.conf` — topologia overlay em JSON (Cenário 3 em malha com 13 nós: 2 servidores `S1` e `S2`, 6 routers overlay `n10`, `n9`, `n6`, `n5`, `n2`, `n1` com links virtuais multi-hop sobre `n8` e `n4`, e 5 clientes `n20`, `n21`, `n22`, `n23`, `n17`).
- `topologias/topology_tp2.xml` — cenário underlay do emulador CORE (25 nós: 10 routers configurados com `FRROSPFv2`/`FRRzebra`/`IPForward`, 5 switches e 10 PCs incluindo `S1` e `S2`).
- `assets/movie.Mjpeg` e `assets/movie2.Mjpeg` — dois ficheiros de vídeo distintos para demonstração multi-stream e multi-servidor.
- `scripts/launch_core.sh` — lê automaticamente os nós de `config/topology.conf` e prepara o Bootstrapper e os nós do overlay no CORE em terminais interativos (sem iniciar fluxos automaticamente; suporta `--auto` e `--auto --with-stream`).
- `scripts/start_node.sh` — atalho para arrancar `boot` ou qualquer nó `ott.py` manualmente dentro de um terminal do CORE.
- `scripts/stop_core.sh` — termina todos os processos ESR TP2 na sessão CORE ativa.
- `scripts/run_all_tests.py` — executa todas as suites automatizadas (suporta `--no-gui`).
- `scripts/demo_video_local.py` — demonstração local rápida do streaming `StreamServer -> VideoClient` sem CORE.
- `tests/test_standalone.py` — testes unitários e de integração do plano de dados (RTP, loop MJPEG, forwarder, pipeline multi-stream e checklist técnica).
- `tests/test_integration_ott.py` — testes de integração controlo↔dados, multicast, troca para rota mais rápida com `DESATIVA`, prevenção de ciclos, failover imediato por menor atraso, deduplicação TCP em links virtuais e validação de `assets/movie2.Mjpeg`.

## Estado de desenvolvimento

- **Concluído e validado ao vivo no CORE (Cenário 3 completo):**
  - Underlay OSPF (FRR) a convergir automaticamente nos 10 routers (`0% packet loss`).
  - Overlay em malha de 13 nós com **2 servidores (`S1` e `S2`) × 2 fluxos distintos cada** (`movie.Mjpeg` e `movie2.Mjpeg`) e ligações virtuais multi-hop (`n10↔n6`, `n6↔n2`, `n1↔n5`).
  - Multicast aplicacional sem duplicação (ex.: `n20` e `n21` em `S1_V1`, `n22` em `S2_V2` simultaneamente), pruning seletivo (`DESATIVA`), `TRACE` com caminho e atraso acumulados, e failover ao vivo confirmado ao encerrar `n9` (`S1 -> n10 -> n9 -> n5 -> n20` → `S1 -> n10 -> n6 -> n5 -> n20` sem queda do vídeo).
  - Bateria automatizada a passar (`3/3` suites).
- **Entregáveis finais pendentes:** redação do relatório LNCS (`ESR-TP2-PLx-Gy-Rel.pdf`, máx. 12 págs.) e empacotamento do ZIP de submissão (`ESR-TP2-PLx-Gy-Codigo.zip`).

## Execução rápida no CORE

1. Abra `topologias/topology_tp2.xml` no CORE, clique em **Start** e aguarde ~10s pela convergência OSPF (FRR).
2. No Ubuntu host (a partir da raiz do projeto), permita X11 e prepare os 13 nós (sem iniciar fluxos automaticamente):
   ```bash
   xhost +local:
   ./scripts/launch_core.sh
   ```
   *(Ou manualmente em cada terminal do CORE: `./scripts/start_node.sh boot` no `S1`, e `./scripts/start_node.sh <node_id>` em cada nó overlay.)*
3. Nos servidores **`S1`** e **`S2`**, lance os fluxos:
   ```text
   # Em S1:
   stream S1_V1 assets/movie.Mjpeg 20
   stream S1_V2 assets/movie2.Mjpeg 20

   # Em S2:
   stream S2_V1 assets/movie.Mjpeg 20
   stream S2_V2 assets/movie2.Mjpeg 20
   ```
4. Nos clientes, abra os vídeos (ex.: `play S1_V1` em `n20` e `n21`, `play S2_V2` em `n22`). Use `vizinhos`, `rotas`, `trace S1_V1`, `ativar`/`desativar` e `sair` (ex.: em `n9`) para validar controlo, multicast e failover.

## Validação local

```bash
python3 scripts/run_all_tests.py
# Em ambientes sem ecrã gráfico:
python3 scripts/run_all_tests.py --no-gui
```
O cliente gráfico requer Pillow (`python3-pil` / `python3-pil.imagetk`) e Tkinter (`python3-tk` no Linux/CORE).
