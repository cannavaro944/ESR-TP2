# Contexto Técnico — ESR TP2

## Arquitetura

O projeto implementa streaming multicast aplicacional sobre uma rede overlay e separa explicitamente a sinalização do transporte de vídeo:

- **Plano de controlo (TCP / JSON — portas `9000` e `9001`):** o bootstrapper (`server.py`, Estratégia 2 da Etapa 1) distribui a lista de vizinhos a cada nó. Os nós overlay (`ott.py`) mantêm ligações TCP full-duplex entre vizinhos com `HELLO` simétrico, medição periódica de latência (`PING`/`PONG` a cada 5s, `RTT / 2`), inundação controlada por iniciativa do servidor (Estratégia 1 da Etapa 2) com mensagens `ANUNCIO` periódicas (10s) contendo métrica acumulada e vetor de caminho (`caminho`) anti-ciclos, ativação inversa (`ATIVA`), pruning seletivo (`DESATIVA`) e teste de percurso (`TRACE`, Etapa 3).
- **Tolerância a falhas e otimização de rotas:** cada nó guarda rotas candidatas (`rotas_candidatas`) por vizinho. Ao descobrir uma rota mais rápida, aplica *make-before-break* (envia `ATIVA` à nova origem e `DESATIVA` à origem antiga, evitando vídeo duplicado). Se a origem atual cair (quebra TCP), `limpar_rotas_vizinho_falhado` purga o nó morto e qualquer candidata que passe por ele (`caminho`) e comuta imediatamente para a rota alternativa viva com menor atraso acumulado.
- **Plano de dados (UDP / RTP — porta `25000`):** frames MJPEG são encapsulados em RTP RFC 3550 (12 bytes, `V=2`, `PT=26`) com o formato no fio `<stream_id>|<cabeçalho RTP 12B><payload JPEG>` (Estratégia 1 da Etapa 4). Os nós intermédios replicam datagramas em bruto (`split(b"|", 1)`) sem descodificar RTP nem JPEG (*zero-decode*).

## Estrutura do repositório

- `src/controlo/server.py` — bootstrapper TCP na porta `9000`; lê `config/topology.conf`.
- `src/controlo/ott.py` — daemon do nó overlay: vizinhos TCP, monitorização de latência, rotas com *path-vector* e failover imediato, CLI interativa e integração com o plano de dados (`obter_destinos_ativos`).
- `src/dados/rtp_packet.py` — codificação e descodificação de cabeçalhos RTP de 12 bytes (`RtpPacket`).
- `src/dados/video_stream.py` — leitor sequencial de ficheiros `.Mjpeg` com repetição automática em loop no EOF (`VideoStream`).
- `src/dados/data_plane.py` — `OverlayForwarder` (encaminhamento multicast UDP *zero-decode*) e `StreamServer` (multi-stream com controlo de framerate anti-drift via `time.perf_counter()`).
- `src/dados/video_client.py` — cliente gráfico Tkinter/Pillow desacoplado via `queue.Queue(maxsize=5)` e fecho limpo (`WM_DELETE_WINDOW` → `DESATIVA` automático).
- `config/topology.conf` — topologia overlay e adjacências em JSON (atualmente Cenário 2 com 6 nós: `S1`, `n10`, `n9`, `n5`, `n20`, `n21`).
- `topologias/topology_tp2.xml` — cenário underlay do emulador CORE (25 nós: 10 routers, 5 switches, 10 PCs).
- `assets/movie.Mjpeg` — ficheiro de vídeo de exemplo.
- `scripts/launch_core.sh` — prepara o Bootstrapper e os nós do overlay no CORE em terminais interativos (sem iniciar fluxos automaticamente; suporta `--auto` e `--auto --with-stream`).
- `scripts/start_node.sh` — atalho para arrancar `boot` ou qualquer nó `ott.py` manualmente dentro de um terminal do CORE.
- `scripts/stop_core.sh` — termina todos os processos ESR TP2 na sessão CORE ativa.
- `scripts/run_all_tests.py` — executa todas as suites automatizadas (suporta `--no-gui`).
- `scripts/demo_video_local.py` — demonstração local rápida do streaming `StreamServer -> VideoClient` sem CORE.
- `tests/test_standalone.py` — testes unitários e de integração do plano de dados (RTP, loop MJPEG, forwarder, pipeline multi-stream e checklist técnica).
- `tests/test_integration_ott.py` — testes de integração controlo↔dados, multicast, troca para rota mais rápida com `DESATIVA`, prevenção de ciclos e failover imediato por menor atraso.

## Estado de desenvolvimento

- **Concluído e validado:** Bootstrapper, ligações TCP full-duplex com `HELLO` e `PING`/`PONG`, anúncios periódicos com métrica de atraso acumulado e vetor `caminho` anti-ciclos, ativação inversa (`ATIVA`), pruning seletivo (`DESATIVA` manual e ao fechar janela), *make-before-break* sem duplicação, failover imediato por menor atraso (`rotas_candidatas`), comando `TRACE`, plano de dados completo (`RtpPacket`, `VideoStream`, `OverlayForwarder`, `StreamServer`, `VideoClient`) e testes automatizados a passar (`3/3` suites).
- **Próximos passos para entrega final:** expandir `config/topology.conf` para o Cenário 3 (malha com caminhos alternativos, mais clientes e 2.º servidor com 2 streams cada), confirmar serviços FRR (`FRROSPFv2`/`FRRzebra`/`IPForward`) em todos os routers de `topologias/topology_tp2.xml`, recolher métricas/capturas nos 3 cenários e redigir o relatório LNCS (`ESR-TP2-PLx-Gy-Rel.pdf`).

## Execução rápida no CORE

1. Abra `topologias/topology_tp2.xml` no CORE, confirme que todos os routers têm `FRROSPFv2` + `FRRzebra` + `IPForward` ativos e inicie a simulação (**Start**).
2. No Ubuntu host (a partir da raiz do projeto), permita X11 e prepare os nós (sem iniciar fluxos automaticamente):
   ```bash
   xhost +local:
   ./scripts/launch_core.sh
   ```
   *(Ou manualmente em cada terminal do CORE: `./scripts/start_node.sh boot` no `S1`, e `./scripts/start_node.sh <node_id>` em cada nó overlay.)*
3. No terminal do servidor (`S1`), lance um ou mais fluxos:
   ```text
   stream Stream1 assets/movie.Mjpeg 20
   ```
4. Nos clientes (`n20`, `n21`, ...), execute `play Stream1`. Use `vizinhos`, `rotas`, `trace Stream1`, `ativar`/`desativar` e `sair` para validar controlo, multicast e tolerância a falhas.

## Validação local

```bash
python3 scripts/run_all_tests.py
# Em ambientes sem ecrã gráfico:
python3 scripts/run_all_tests.py --no-gui
```
O cliente gráfico requer Pillow (`python3-pil` / `python3-pil.imagetk`) e Tkinter (`python3-tk` no Linux/CORE).
