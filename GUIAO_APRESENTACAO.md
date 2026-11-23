# Guião Completo de Apresentação e Explicação (3 Pessoas) — ESR TP2

> **Objetivo deste guião:** orientar os **3 elementos do grupo** durante a defesa ao docente, alinhado ponto por ponto com o enunciado (*Trabalho Prático Nº 2 — Serviço Over the Top para entrega de multimédia*).  
> Inclui **justificação pormenorizada das tomadas de decisão** (TCP vs UDP, Estratégia 1 vs Estratégia 2 em cada etapa), **divisão clara de papéis para 3 pessoas**, **como demonstrar todas as funcionalidades no terminal/CORE**, **apreciação crítica** e **respostas às perguntas do professor**.

---

## 1. Divisão da Apresentação para 3 Pessoas (Alinhada com a Avaliação)

A apresentação está pensada para **12–15 minutos**, cobrindo os 5 itens avaliados no enunciado:

| Elemento | Responsabilidade na Defesa | Etapas & Pesos no Enunciado | Ficheiros a Referir |
|---|---|---|---|
| **Pessoa 1** | Visão geral (Underlay vs Overlay), escolha **TCP vs UDP**, formação do Overlay (**Estratégia 2: Bootstrapper**), `HELLO` simétrico e monitorização de latência (`PING`/`PONG`) | **Etapa 0** + **Etapa 1** — Topologia da Rede Overlay *(10%)* + base do Controlo | `topologias/topology_tp2.xml`, `config/topology.conf`, `src/controlo/server.py`, `src/controlo/ott.py` |
| **Pessoa 2** | Encaminhamento dinâmico (**Estratégia 1: anúncios do servidor**), métrica de atraso acumulado, prevenção de ciclos (*path-vector*), ativação inversa (`ATIVA`), pruning (`DESATIVA`), `TRACE` e **failover imediato por menor atraso** | **Etapa 2** + **Etapa 3** — Protocolo de Controlo *(15%)* + Gestão de Fluxos *(25%)* | `src/controlo/ott.py` |
| **Pessoa 3** | Plano de Dados (**Estratégia 1: RTP/MJPEG**), encapsulamento `RtpPacket`, loop contínuo `VideoStream`, replicação multicast *zero-decode* (`OverlayForwarder`), `StreamServer` anti-drift multi-stream, `VideoClient` Tkinter/Pillow, testes automatizados e apreciação crítica | **Etapa 4** + Integração — Protocolo Streaming *(10%)* + Solução Integrada *(30%)* | `src/dados/rtp_packet.py`, `src/dados/video_stream.py`, `src/dados/data_plane.py`, `src/dados/video_client.py`, `tests/` |

---

## 2. Tomadas de Decisão Arquiteturais (De Acordo com o Enunciado)

Esta é uma das partes mais valorizadas na defesa (**subitem a. Especificação conceptual** e **subitem c. Apreciação crítica**). Cada pessoa deve justificar **porquê esta escolha** em vez das alternativas do enunciado.

### 2.1. Etapa 0 — Protocolo de Transporte: Porquê Arquitetura Híbrida (TCP + UDP)? *(Pessoa 1)*

O enunciado pede para definir se o overlay usa **TCP**, **UDP** ou **HTTP**. Em vez de usar um único protocolo para tudo, separámos o sistema em **dois planos coordenados**:

| Plano | Protocolo Escolhido | Porque escolhemos este | Porque descartámos as alternativas |
|---|---|---|---|
| **Plano de Controlo** (`server.py` porta `9000`, `ott.py` porta `9001`) | **TCP** com mensagens **JSON** delimitadas por `\n` | • Entrega garantida e ordenada de mensagens críticas (`HELLO`, `ANUNCIO`, `ATIVA`, `DESATIVA`, `TRACE`).<br>• **Deteção imediata de falha de vizinho:** quando um nó morre ou a ligação cai, o socket TCP fecha e `recv()` devolve `b""`/exceção quase instantaneamente, disparando o failover sem esperar por timeouts longos. | • **UDP no controlo** obrigaria a reimplementar ACKs, retransmissões e *heartbeats* de timeout para não perder `ATIVA`/`DESATIVA`.<br>• **HTTP** introduz sobrecarga de cabeçalhos e modelo pedido/resposta pouco natural para sinalização *full-duplex* assíncrona. |
| **Plano de Dados** (`data_plane.py`, `video_client.py` porta `25000`) | **UDP** com encapsulamento **RTP** (RFC 3550) | • Streaming de vídeo tem requisitos de **tempo real**: atrasar frames futuros para retransmitir um frame perdido (*head-of-line blocking* do TCP) piora a fluidez.<br>• Baixo overhead e replicação multicast 1→N muito rápida em cada nó intermédio (`sendto` para cada destino ativo). | • **TCP nos dados** causa bloqueios em cascata quando há perdas e exige manter buffers/estados de janela por cada vizinho de saída.<br>• **HTTP** adiciona latência e não se adequa a multicast aplicacional frame-a-frame. |

---

### 2.2. Etapa 1 — Construção do Overlay: Porquê Estratégia 2 (Bootstrapper) em vez da Estratégia 1 (Manual)? *(Pessoa 1)*

| Critério | Estratégia 1 (Manual — `ott <viz1> <viz2> ...`) | **Estratégia 2 (Controlador / Bootstrapper — ESCOLHIDA)** |
|---|---|---|
| **Como funciona** | Cada nó recebe todos os IPs dos vizinhos na linha de comandos ao arrancar. | Cada nó recebe apenas o seu `node_id` e o IP do Bootstrapper (`server.py:9000`), que lê `config/topology.conf` e devolve a lista de vizinhos. |
| **Vantagens** | Sem nó central; não existe ponto único de falha no arranque. | • Comando uniforme e rápido em todos os nós (`ott <id> <boot_ip> <porta>`).<br>• Topologia inteira centralizada num único JSON (`config/topology.conf`), fácil de escalar dos Cenários 1 e 2 para o Cenário 3 sem erros de IPs.<br>• Facilita a gestão consistente da simetria de vizinhança. |
| **Gestão de Abandono** | Detetado por quebra de ligação TCP. | **Igual:** após o registo inicial, os nós comunicam **diretamente entre si em TCP full-duplex**. Se um nó abandonar, os vizinhos detetam imediatamente pela quebra TCP (o Bootstrapper não fica no caminho crítico do tráfego nem das falhas). |
| **Compromisso (crítica)** | Difícil de manter em redes grandes. | O Bootstrapper é ponto único de falha **apenas no momento do arranque** de novos nós (limitação mitigável com replicação do Bootstrapper em trabalho futuro). |

> **Nota do enunciado (pág. 2) a mencionar ao professor:** No CORE todos os routers são *containers* Linux, pelo que conseguimos instanciar nós overlay tanto em PCs como em routers para testar; contudo, numa rede física real, os nós overlay correm apenas em sistemas terminais/servidores dedicados, e não no firmware de routers/switches comerciais.

---

### 2.3. Etapa 2 — Construção de Rotas: Porquê Estratégia 1 (Iniciativa do Servidor) + Métrica de Atraso? *(Pessoa 2)*

| Critério | **Estratégia 1 (Iniciativa do Servidor — ESCOLHIDA)** | Estratégia 2 (Iniciativa do Cliente) |
|---|---|---|
| **Mecanismo** | O servidor envia `ANUNCIO` periódico (a cada 10s) com inundação controlada; as rotas ficam `Ativa: False` até um cliente enviar `ATIVA` pelo caminho inverso (`Origem`). | Cada cliente inunda a rede (ou contacta um servidor que conhece todo o grafo) a pedir rota para o stream. |
| **Porque escolhemos** | • **Escalabilidade e descentralização:** nenhum nó precisa de conhecer o grafo completo do overlay.<br>• **Zero tráfego de vídeo desnecessário:** sem clientes, as tabelas existem mas estão inativas (`Ativa: False`).<br>• **Adaptação contínua:** novos clientes encontram logo a rota pronta na tabela e alterações de latência/topologia propagam-se automaticamente. | • Se o servidor centralizar o cálculo de rotas, torna-se gargalo e precisa de monitorizar toda a rede.<br>• Se os clientes inundarem a rede, N clientes geram N inundações de pedidos. |

**Decisões-chave dentro da Estratégia 1 (diferenciadores do nosso grupo):**
1. **Métrica de Atraso Real (`RTT / 2` em ms) em vez de Nº de Saltos:**
   - Um caminho com menos saltos pode passar por links congestionados ou lentos. Cada nó mede o atraso real para cada vizinho de 5 em 5s via `PING`/`PONG` e acumula esse custo em cada salto (`nova_metrica = msg["metrica"] + custo_link`).
2. **Prevenção de Ciclos (*Path-Vector*):**
   - Cada `ANUNCIO` transporta `"caminho": [S1, n10, ...]` além de `seq` e `metrica`. Se um nó vir o seu próprio `meu_id` no `caminho`, descarta imediatamente — eliminando qualquer risco de ciclo em malhas complexas.
3. **Troca sem Duplicados (*Make-Before-Break*):**
   - Quando aparece uma rota com menor atraso para um fluxo já ativo, o nó envia primeiro `ATIVA` para a **nova origem** e imediatamente a seguir `DESATIVA` para a **origem antiga**, evitando vídeo duplicado e minimizando perdas na transição.
4. **Failover Instantâneo (`rotas_candidatas`):**
   - Cada nó guarda em cache as rotas alternativas anunciadas por outros vizinhos. Se a origem atual cair (quebra TCP), `limpar_rotas_vizinho_falhado` remove o nó morto e qualquer rota alternativa que passasse por ele (`caminho`) e **comuta imediatamente** para a rota viva com **menor atraso acumulado**, enviando `ATIVA` sem esperar até 10s pela próxima ronda de anúncios.

---

### 2.4. Etapa 4 — Streaming: Porquê Estratégia 1 (Baseada em Kurose — RTP/MJPEG) em vez de Alternativas? *(Pessoa 3)*

| Critério | **Estratégia 1 (RTP + MJPEG adaptada — ESCOLHIDA)** | Estratégia 2 (Alternativas: HTTP/DASH, RTSP/FFmpeg) |
|---|---|---|
| **Porque escolhemos** | • Alinhada diretamente com o material de referência da UC (Kurose [1] e `movie.Mjpeg` [2]).<br>• **Controlo total sobre o protocolo:** implementámos cabeçalho RTP RFC 3550 real de 12 bytes (`V=2`, `PT=26` JPEG, sequência cíclica 16 bits, timestamp 32 bits em ms, SSRC).<br>• **Multiplexagem e Zero-Decode nos intermédios:** formato no fio `<stream_id>\|<RTP 12B><JPEG>` permite múltiplos streams na mesma porta UDP `25000` e reencaminhamento em bruto (`split(b"|", 1)`) **sem usar Pillow nem descodificar JPEG/RTP** nos nós intermédios. | • Soluções como FFmpeg/DASH escondem os detalhes do transporte/empacotamento (menos transparência pedagógica) e trazem dependências pesadas para os containers CORE. |

**Decisões técnicas importantes no Plano de Dados:**
- **Loop contínuo (`VideoStream`):** quando `movie.Mjpeg` (~500 frames / ~25s) chega ao EOF, faz `seek(0)` automático sem interromper a demonstração.
- **Controlo de framerate sem *drift* (`StreamServer`):** usa `time.perf_counter()` para descontar o tempo de leitura de disco e envio UDP (`max(0.0, frame_interval - elapsed)`), mantendo 20 fps estáveis, e só lê/envia frames se `obter_destinos_ativos(stream_id)` não estiver vazio.
- **Interface gráfica fluida (`VideoClient`):** separa a thread de receção UDP (buffer de `65536` bytes + `SO_REUSEADDR`/`SO_REUSEPORT`) da thread principal Tkinter através de uma `queue.Queue(maxsize=5)` que descarta frames velhos se a GUI atrasar, e captura o fecho da janela (`WM_DELETE_WINDOW`) para disparar `DESATIVA` automaticamente.

---

## 3. Preparação ANTES do Professor Chegar (5 min antes)

1. **Permitir janelas gráficas dos containers CORE no Ubuntu host:**
   ```bash
   xhost +local:
   ```
2. **Abrir e arrancar o cenário no CORE:**
   - Abrir `topologias/topology_tp2.xml` no CORE.
   - *(Confirmar no CORE que todos os routers têm OSPF IPv4 operacional — atenção ao router `n9`, verificar se está com `OSPFv2`.)*
   - Clicar em **Start** (botão verde) e aguardar ~3 segundos pela convergência das rotas IP da underlay.
3. **Preparar os nós do Overlay (SEM iniciar nenhum fluxo ainda!):**
   - Na pasta raiz do projeto:
     ```bash
     chmod +x scripts/*.sh
     ./scripts/launch_core.sh
     ```
   - O `launch_core.sh` abre terminais limpos (`Boot (S1)`, `S1 (Srv)`, `n10`, `n9`, `n5`, `n20`, `n21`) já ligados ao Bootstrapper e com vizinhos TCP estabelecidos, **mas sem iniciar nenhum stream** — deixando tudo pronto para demonstrar cada comando ao vivo!
   - *(Se preferirem arrancar tudo à mão dentro dos terminais de cada nó no CORE, usem: `./scripts/start_node.sh boot`, `./scripts/start_node.sh S1`, `./scripts/start_node.sh n10`, etc., ou diretamente `python3 -m src.controlo.server` e `python3 -m src.controlo.ott <id> <boot_ip> 9001`.)*
4. **Confirmar testes automatizados previamente:**
   ```bash
   python3 scripts/run_all_tests.py --no-gui
   ```

---

## 4. Guia de Comandos Integrados no Terminal (`ott.py`)

Referência rápida de **todas as funcionalidades integradas** que podem ser mostradas ao vivo no prompt `[<node_id>]>`:

| Comando no Terminal | Onde Executar | Funcionalidade Demonstrada |
|---|---|---|
| `vizinhos` | Qualquer nó | Ligações TCP full-duplex ativas + latência em ms medida via `PING`/`PONG` (Etapa 1) |
| `anunciar <fluxo>` | Servidor (`S1`, `S2`) | Anúncio periódico de controlo sem vídeo (para testar só plano de controlo / rotas) |
| `stream <fluxo> <ficheiro> [fps]` | Servidor (`S1`, `S2`) | Regista stream no `StreamServer` + inicia `ANUNCIO` periódico com `caminho` (Etapas 2 e 4; pode correr 2+ vezes para múltiplos streams!) |
| `rotas` | Qualquer nó | Tabela de rotas completa: `Fluxo`, `Srv`, `Origem`, `Atraso (ms)`, `Destinos`, `Ativa` (Etapa 2) |
| `ativar <fluxo>` | Cliente / Intermédio | Envia `ATIVA` manualmente pelo caminho inverso sem abrir GUI (útil para provar controlo puro) |
| `desativar <fluxo>` | Cliente / Intermédio | Envia `DESATIVA` (pruning multicast) manualmente pelo terminal |
| `play <fluxo>` | Cliente (`n20`, `n21`, ...) | Envia `ATIVA` + abre janela gráfica `VideoClient` (UDP `25000`); ao fechar janela envia `DESATIVA` automático (Etapa 4) |
| `trace <fluxo>` | Servidor (`S1`, `S2`) | Envia mensagem `TRACE` pelas rotas ativas, registando log e caminho acumulado até aos destinos (Etapa 3) |
| `sair` | Qualquer nó | Encerra o nó de forma limpa (simula saída/falha de nó e dispara remoção de rotas + failover nos vizinhos) |

**Comandos de apoio no sistema Linux (para prova empírica de rede):**
- `tcpdump -n -i any udp port 25000` (num nó do CORE) → comprova **0 pacotes** antes de `play` e **frames RTP/UDP reais** durante `play`, além de confirmar **1 único fluxo** na ligação `S1 ↔ n10` vs **2 cópias** à saída de `n5`.
- `python3 scripts/run_all_tests.py` → executa todas as suites automatizadas (RTP, loop, forwarder, pipeline multi-stream, multicast, failover imediato, anti-ciclos, GUI).

---

## 5. Roteiro Passo a Passo ao Vivo (3 Pessoas — 6 Atos)

### 🎬 ATO 1 — Arquitetura, Decisões de Transporte e Formação do Overlay *(Pessoa 1 — ~3 min)*

1. **Explicação inicial (olhando para o CORE):**
   - Explicar a separação **Underlay** (rede IP no CORE com routers OSPF) vs **Overlay** (rede lógica aplicacional apenas nos nós escolhidos).
   - Justificar a **arquitetura híbrida TCP (controlo) + UDP (dados)** (Secção 2.1) e a escolha da **Estratégia 2 (Bootstrapper)** face à Estratégia 1 manual (Secção 2.2).
2. **Comandos a mostrar no terminal:**
   - Mostrar o separador **`Boot (S1)`** com o registo dos nós:
     ```text
     [*] Bootstrapper S1 a escutar na porta 9000...
     [*] Nó S1 registou-se a partir de ('10.0.19.21', ...)
     [*] Nó n10 registou-se a partir de ('10.0.19.1', ...)
     ```
   - Nos separadores **`S1`** e **`n5`**, correr:
     ```text
     vizinhos
     ```
3. **Output esperado:**
   ```text
   -> 10.0.12.2 (atraso: 0.412 ms)
   -> 10.0.18.20 (atraso: 0.385 ms)
   -> 10.0.18.21 (atraso: 0.401 ms)
   ```
4. **O que destacar ao professor:**
   > *"Cada nó contactou o Bootstrapper na porta TCP 9000, recebeu apenas os seus vizinhos do `topology.conf` e estabeleceu ligações TCP full-duplex com `HELLO` bidirecional. Como se vê em `vizinhos`, o atraso real (`RTT/2`) de cada link é medido continuamente a cada 5 segundos via `PING`/`PONG`."*

---

### 🎬 ATO 2 — Anúncio de Fluxos, Tabela de Rotas Inativa e Múltiplos Streams *(Pessoa 2 — ~2,5 min)*

1. **Explicação antes do comando:**
   - Justificar a **Estratégia 1 (iniciativa do servidor)** face à Estratégia 2 (iniciativa do cliente) e a escolha da **métrica de atraso acumulado em ms** + *path-vector* (`caminho`) anti-ciclos (Secção 2.3).
2. **Comandos a mostrar no terminal:**
   - No separador **`S1`**, iniciar **dois fluxos distintos** (mostrando suporte multi-stream pedido no enunciado):
     ```text
     stream Stream1 assets/movie.Mjpeg 20
     stream Stream2 assets/movie.Mjpeg 15
     ```
     *(Se tiverem um 2.º servidor `S2` configurado no Cenário 3, correr também `stream S2_Stream1 assets/movie.Mjpeg 20` e `stream S2_Stream2 assets/movie.Mjpeg 15` em `S2`!)*
   - Nos separadores **`n5`** e **`n20`**, correr:
     ```text
     rotas
     ```
   - *(Opcional — prova forte)* Num terminal de **`n5`**, abrir rapidamente `tcpdump -n -i any udp port 25000` durante 2 segundos para mostrar **0 pacotes UDP** a circular.
3. **Output esperado em `n5` / `n20`:**
   ```text
   Fluxo: Stream1 (Srv: S1) | Origem: 10.0.12.2 | Atraso: 1.245 ms | Destinos: [] | Ativa: False
   Fluxo: Stream2 (Srv: S1) | Origem: 10.0.12.2 | Atraso: 1.245 ms | Destinos: [] | Ativa: False
   ```
4. **O que destacar ao professor:**
   > *"Os anúncios periódicos propagaram-se por inundação controlada sem ciclos e preencheram as tabelas com o atraso acumulado até `S1`. Reparem que `Ativa: False` e `Destinos: []`: ainda não há qualquer tráfego de vídeo UDP na rede porque nenhum cliente pediu os streams."*

---

### 🎬 ATO 3 — Plano de Dados, 1.º Cliente e Encapsulamento RTP *(Pessoa 3 — ~2,5 min)*

1. **Explicação antes do comando:**
   - Justificar a **Estratégia 1 (RTP + MJPEG)**, o formato no fio `<stream_id>|<RTP 12B><JPEG>`, o reencaminhamento *zero-decode* nos nós intermédios e o controlo de framerate anti-drift (`perf_counter()`).
2. **Comandos a mostrar no terminal:**
   - No separador do 1.º cliente (**`n20`**), correr:
     ```text
     play Stream1
     ```
3. **Output esperado:**
   - Nos terminais **`n5` → `n9` → `n10` → `S1`** vê-se o `ATIVA` a subir pelo caminho inverso:
     ```text
     [*] Rota Stream1 ATIVADA por 10.0.18.20. A propagar para 10.0.12.2...
     ```
   - Abre-se a janela **`ESR Video Client — Stream: Stream1`** em `n20` com o vídeo a reproduzir e a barra inferior a mostrar `Seq`, `Frames`, `Recebidos` e `Bytes` em tempo real (e passado ~25s repete em loop sem quebrar!).
4. **O que destacar ao professor:**
   > *"O pedido `ATIVA` subiu seguindo o campo `Origem` de cada tabela até `S1`. Só aí o `StreamServer` começou a enviar pacotes RTP/UDP na porta 25000. Nos nós intermédios (`n10`, `n9`, `n5`), o `OverlayForwarder` lê apenas o prefixo `Stream1|` e reencaminha os bytes brutos sem abrir o JPEG. No `n20`, o `VideoClient` descodifica numa thread dedicada e passa à GUI Tkinter via `queue.Queue` para nunca bloquear."*

---

### 🎬 ATO 4 — Multicast Aplicacional (2+ Clientes), Multi-Stream e `TRACE` *(Pessoa 2 + Pessoa 3 — ~3 min)*

1. **Comandos a mostrar no terminal:**
   - **(Pessoa 3)** No 2.º cliente (**`n21`**), correr:
     ```text
     play Stream1
     ```
     *(Ficam **ambas as janelas de vídeo abertas em simultâneo**! Se tiverem mais clientes no Cenário 3, podem abrir `play Stream2` noutro cliente para mostrar 2 fluxos em paralelo na mesma porta `25000`.)*
   - **(Pessoa 3)** Mostrar a prova de **multicast sem duplicação** correndo `rotas` em **`S1`** e em **`n5`**:
     ```text
     rotas
     ```
   - **(Pessoa 2)** Demonstrar a **Etapa 3 (Teste de Percursos)** correndo em **`S1`**:
     ```text
     trace Stream1
     ```
2. **Output esperado:**
   - Em **`S1`**: `Destinos: ['10.0.19.1'] | Ativa: True` → **apenas 1 cópia sai do servidor!**
   - Em **`n5`**: `Destinos: ['10.0.18.20', '10.0.18.21'] | Ativa: True` → **a replicação só acontece no ponto de ramificação!**
   - Nos nós durante `trace Stream1`:
     ```text
     [TRACE-LOG] Fluxo: Stream1 | Caminho acumulado: S1 -> n10 -> n9 -> n5 | Atraso até aqui: 1.245 ms | Rota Ativa: True
     [TRACE-FIM] O teste de percurso para 'Stream1' chegou ao destino final (n20)! Percurso: S1 -> n10 -> n9 -> n5 -> n20
     [TRACE-FIM] O teste de percurso para 'Stream1' chegou ao destino final (n21)! Percurso: S1 -> n10 -> n9 -> n5 -> n21
     ```
3. **O que destacar ao professor:**
   - **Pessoa 3:** *"Ao contrário da solução naïve em que o servidor envia N fluxos, `S1` envia apenas 1 fluxo até `n5`, onde o `OverlayForwarder` replica o datagrama UDP para `n20` e `n21`."*
   - **Pessoa 2:** *"O comando `trace` implementa a Etapa 3: segue exatamente a árvore ativa de distribuição, regista logs com o atraso acumulado em cada salto e confirma todos os caminhos finais até aos clientes."*

---

### 🎬 ATO 5 — Pruning Multicast (`DESATIVA` Automático e Manual) *(Pessoa 3 + Pessoa 2 — ~1,5 min)*

1. **Comandos / ações a mostrar:**
   - **(Pessoa 3)** Fechar a janela de vídeo de **`n20`** no botão **`X`** (ou executar `desativar Stream1` no terminal de `n20`).
   - Mostrar em **`n5`** (`rotas`) que `n20` saiu de `Destinos` mas **`n21` continua a ver o vídeo sem interrupção**.
   - Fechar agora a janela de **`n21`** no botão **`X`**.
   - Executar `rotas` em **`n5`** e **`S1`**.
2. **Output esperado:**
   - Em `n20`: `[*] VideoClient fechado. Rota Stream1 DESATIVADA (sem destinos restantes).`
   - Em `n5` após fechar `n20`: `Destinos: ['10.0.18.21'] | Ativa: True`
   - Em `n5` e `S1` após fechar `n21`:
     ```text
     [*] Rota Stream1 DESATIVADA (sem destinos restantes).
     Fluxo: Stream1 (Srv: S1) | Origem: LOCAL | Atraso: 0.0 ms | Destinos: [] | Ativa: False
     ```
3. **O que destacar ao professor:**
   > *"O pruning é seletivo: fechar um cliente poda apenas esse ramo sem afetar os restantes. Só quando o último cliente sai é que `DESATIVA` sobe até `S1`, desligando imediatamente a transmissão UDP."*

---

### 🎬 ATO 6 — Tolerância a Falhas, Re-roteamento por Menor Atraso e Testes Automatizados *(Pessoa 2 + Pessoa 3 — ~2 min)*

1. **Ao vivo no CORE (quando estiverem no Cenário 3 com caminhos alternativos):**
   - Com `n20` a reproduzir `Stream1`, ir ao nó intermédio da rota principal e escrever:
     ```text
     sair
     ```
     *(ou desligar o link diretamente no CORE)*.
   - **Output esperado nos vizinhos:**
     ```text
     [-] Vizinho <IP_antigo> desligou-se (rotas associadas atualizadas).
     [FAILOVER] Origem (<IP_antigo>) falhou! Rota 'Stream1' comutada para <IP_novo> (Atraso: ... ms).
     ```
   - Mostrar que o `VideoClient` em `n20` retoma o vídeo automaticamente pela nova rota de menor atraso e que `rotas` já reflete a nova `Origem`!
   - *(Também podem mostrar o comando `ativar Stream1` / `desativar Stream1` manualmente num terminal sem GUI para provar o plano de controlo isolado.)*
2. **Validação automatizada no terminal (útil especialmente se estiverem ainda no cenário linear de 6 nós):**
   - **(Pessoa 3)** Correr na raiz do projeto:
     ```bash
     python3 scripts/run_all_tests.py --no-gui
     ```
   - Mostrar que **4/4 suites passam**, incluindo especificamente o teste em `test_integration_ott.py` que valida:
     - Multicast para 2 clientes;
     - Troca para rota mais rápida enviando `ATIVA` à nova origem + `DESATIVA` à origem antiga (sem duplicados);
     - Rejeição de ciclos (*path-vector*);
     - Queda do nó principal com failover imediato para a rota alternativa de menor atraso (descartando rotas que passavam pelo nó morto).

---

## 6. Apreciação Crítica e Trabalho Futuro *(Critério `c.` da Avaliação — todas as pessoas)*

Mostrar espírito crítico no final valoriza muito a nota:

- **Pontos fortes da solução:**
  - Separação limpa Controlo (TCP) vs Dados (UDP/RTP), sem descodificação nos intermédios (*zero-decode*).
  - Métrica dinâmica real (`RTT/2`) em vez de saltos fixos, com *path-vector* anti-ciclos, *make-before-break* sem duplicados e failover imediato por cache de rotas candidatas.
  - Suporte nativo a múltiplos servidores e múltiplos fluxos simultâneos na mesma porta UDP `25000`.
- **Limitações identificadas e trabalho futuro:**
  1. **Bootstrapper estático (SPOF no arranque):** `server.py` lê `config/topology.conf` no arranque. Como trabalho futuro, suportar descoberta distribuída (ou Bootstrapper redundante) e entrada dinâmica de nós não listados previamente.
  2. **Partilha da porta UDP `25000` em nós que fazem *forwarding* e `play` simultaneamente:** atualmente `_executar_play` pára o `OverlayForwarder` local para o `VideoClient` fazer *bind* na porta `25000`, o que funciona perfeitamente em nós folha (clientes), mas impediria um nó intermédio de encaminhar para outros enquanto exibe vídeo localmente. Solução futura: encaminhar via `127.0.0.1` para uma porta local dedicada do `VideoClient` (ex.: `25001`).
  3. **Métrica baseada apenas em latência:** em trabalho futuro, combinar atraso (`RTT/2`) com largura de banda disponível e taxa de perda de pacotes RTP (usando o número de sequência RTP / RTCP) para evitar links com baixo atraso mas congestionados.

---

## 7. Perguntas Clássicas do Professor (Quem Responde)

| Pergunta do Professor | Quem Responde | Resposta Curta e Precisa |
|---|---|---|
| **"Porque usaram TCP no controlo e UDP nos dados?"** | **Pessoa 1** | Controlo exige fiabilidade e deteção imediata de queda de vizinho por quebra do socket TCP. Vídeo exige baixa latência em tempo real sem *head-of-line blocking* do TCP e replicação UDP rápida 1→N. |
| **"Porque escolheram Estratégia 2 no overlay e não manual?"** | **Pessoa 1** | Centraliza a topologia num único ficheiro (`topology.conf`), simplifica o arranque de dezenas de nós sem erros de IPs e mantém o abandono distribuído via TCP direto entre vizinhos. |
| **"Porque anúncios do servidor (Estratégia 1) e não pedido do cliente?"** | **Pessoa 2** | Evita inundação por cada cliente novo e dispensa um controlador que conheça todo o grafo; sem clientes a rota fica `Ativa: False` (zero tráfego UDP) e a árvore adapta-se dinamicamente. |
| **"Como evitam ciclos e pacotes duplicados ao trocar de rota?"** | **Pessoa 2** | Cada `ANUNCIO` leva o vetor `caminho` (se o nó já estiver lá, descarta) e, ao mudar de origem ativa, fazemos *make-before-break*: enviamos `ATIVA` à nova origem e `DESATIVA` à antiga. |
| **"O que acontece se um nó intermédio morrer a meio do vídeo?"** | **Pessoa 2** | O TCP fecha de imediato, `limpar_rotas_vizinho_falhado` purga o nó morto (e rotas que passavam por ele) e comuta logo para a rota alternativa viva de menor atraso em `rotas_candidatas`, enviando `ATIVA` sem esperar 10s. |
| **"Os nós intermédios abrem a imagem JPEG?"** | **Pessoa 3** | Não. O datagrama é `<stream_id>\|<RTP 12B><JPEG>`. O `OverlayForwarder` faz apenas `split(b"|", 1)` para identificar o fluxo e reenvia os bytes brutos (zero Pillow, zero descodificação RTP/JPEG). |
| **"Porque `65536` no `recvfrom` e `queue.Queue(maxsize=5)` na GUI?"** | **Pessoa 3** | Frames MJPEG podem exceder 20–30 KB (buffer grande evita truncar frames); a fila limitada desacopla a rede da thread Tkinter e descarta frames antigos se a GUI atrasar, mantendo o vídeo em tempo real sem bloquear. |
| **"Como suportam 2 servidores e 2 streams cada?"** | **Pessoa 3** | Qualquer nó `ott.py` pode correr `stream <id> <ficheiro> [fps]` várias vezes: o `StreamServer` cria 1 thread por `stream_id`, cada pacote leva o prefixo `<stream_id>\|` e cada fluxo tem entrada independente em `tabela_rotas`. |

---

## 8. Plano B — Se Algo Correr Mal ao Vivo

- **Se o CORE ficar com processos presos ou precisar de arrancar tudo automaticamente em background:**
  ```bash
  ./scripts/stop_core.sh
  xhost +local:
  ./scripts/launch_core.sh --auto --with-stream
  ```
- **Se o ambiente gráfico X11/WSLg falhar no CORE:**
  - Mostrar todo o plano de controlo e multicast no CORE (`vizinhos`, `rotas`, `trace`, `tcpdump -n -i any udp port 25000`) e demonstrar o player gráfico localmente no host com:
    ```bash
    python3 scripts/demo_video_local.py
    ```
  - E correr a bateria completa de verificação:
    ```bash
    python3 scripts/run_all_tests.py
    ```
