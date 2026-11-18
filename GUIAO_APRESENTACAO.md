# Guião Completo de Apresentação — ESR TP2

> **Objetivo deste guião:** servir como "cábula" prática durante a demonstração ao docente.  
> Indica **o que preparar antes**, **a ordem dos pontos a mostrar**, **que comando escrever**, **o que vai aparecer no ecrã** e **o que dizer ao professor**.

---

## 0. O Que Preparar ANTES do Professor Chegar (5 minutos antes)

Façam isto antes de começar a apresentar para não perderem tempo com erros de ambiente:

1. **Permitir janelas gráficas do CORE no Ubuntu host:**
   ```bash
   xhost +local:
   ```
2. **Abrir o cenário no CORE:**
   - Abrir `topologias/topology_tp2.xml` no CORE.
   - *(Confirmar no CORE que o router `n9` tem `OSPFv2` ativo caso `n5`/`n20`/`n21` precisem de rota IPv4 até ao Bootstrapper `10.0.19.21`.)*
   - Carregar no botão **Start** (verde) e esperar ~3 segundos para o OSPF convergir.
3. **Arrancar a rede Overlay com os terminais organizados:**
   - Na pasta do projeto (no Ubuntu host), correr:
     ```bash
     chmod +x scripts/*.sh
     ./scripts/launch_core.sh
     ```
   - Vão abrir separadores limpos (`Boot (S1)`, `S1 (Srv)`, `n10`, `n9`, `n5`, `n20`, `n21`) **sem terem de escrever comandos gigantes**.
   - *(Se preferirem arrancar tudo à mão nos terminais dos nós no CORE, usem o atalho `./scripts/start_node.sh boot`, `./scripts/start_node.sh S1`, `./scripts/start_node.sh n10`, etc.)*
4. **Confirmar que o `Stream1` ainda NÃO tem clientes ligados** (se já testaram antes da defesa, fechem as janelas de vídeo para começar do zero — com `Ativa: False`).
5. **Validar em segundo plano (opcional, antes da aula):**
   ```bash
   python3 scripts/run_all_tests.py
   ```
   *(Mostra 4/4 suites aprovadas, útil se o professor perguntar por testes automatizados.)*

---

## 1. Resumo dos Módulos a Apresentar

| Bloco | O que Apresentar | Ficheiros a referir se o professor perguntar |
|---|---|---|
| **Topologia e Vizinhança** | Topologia Overlay, Bootstrapper e Monitorização TCP (`HELLO`, `PING`/`PONG`) | `config/topology.conf`, `src/controlo/server.py`, `src/controlo/ott.py` |
| **Controlo e Encaminhamento** | Anúncios periódicos, Tabela de Rotas, Ativação Inversa, `TRACE` e Tolerância a Falhas | `src/controlo/ott.py` |
| **Plano de Dados e Streaming** | RTP/UDP, Multicast Overlay sem duplicação, Loop MJPEG, Cliente Gráfico e Pruning (`DESATIVA`) | `src/dados/rtp_packet.py`, `src/dados/video_stream.py`, `src/dados/data_plane.py`, `src/dados/video_client.py` |

---

## 2. Roteiro Passo a Passo (Ordem Exata ao Vivo)

### 🎬 ATO 1 — Arquitetura e Formação da Rede Overlay *(~2 min)*

1. **O que mostrar:**
   - O mapa da topologia no **CORE** e o separador **`Boot (S1)`**.
   - Nos separadores de **`S1`** e **`n5`**, escrever:
     ```text
     vizinhos
     ```
2. **O que vai aparecer no ecrã:**
   - No `Boot (S1)`:
     ```text
     [*] Bootstrapper S1 a escutar na porta 9000...
     [*] Nó S1 registou-se a partir de ...
     [*] Nó n10 registou-se a partir de ...
     ```
   - No terminal de `S1` / `n5` ao correr `vizinhos`:
     ```text
     -> 10.0.12.2 (atraso: 0.412 ms)
     -> 10.0.18.20 (atraso: 0.385 ms)
     -> 10.0.18.21 (atraso: 0.401 ms)
     ```
3. **O que dizer ao professor:**
   > *"Temos uma rede física (underlay) no CORE e por cima construímos uma rede Overlay. Ao arrancar, cada nó contacta o nosso Bootstrapper em TCP na porta 9000 (`server.py`), que lê o `topology.conf` e devolve apenas os seus vizinhos no overlay. Depois os nós estabelecem ligações TCP full-duplex entre si, identificam-se com `HELLO` e enviam `PING`/`PONG` de 5 em 5 segundos para medir a latência (`RTT / 2`) de cada link, visível no comando `vizinhos`."*

---

### 🎬 ATO 2 — Anúncio de Streams e Tabela de Rotas Inativa *(~2 min)*

1. **O que fazer:**
   - Se o `S1` ainda não iniciou o stream (no modo `--stream` já arranca pronto), escrever no separador **`S1`**:
     ```text
     stream Stream1 assets/movie.Mjpeg 20
     ```
   - Ir ao separador de um nó intermédio (**`n5`**) e de um cliente (**`n20`**) e escrever:
     ```text
     rotas
     ```
2. **O que vai aparecer no ecrã:**
   - Durante a propagação inicial aparece:
     ```text
     [*] Rota descoberta: Stream1 via 10.0.12.2 (Atraso: 1.245 ms | Ronda: 1)
     ```
   - Ao correr `rotas` em `n5` e `n20`:
     ```text
     Fluxo: Stream1 (Srv: S1) | Origem: 10.0.12.2 | Atraso: 1.245 ms | Destinos: [] | Ativa: False
     ```
3. **O que dizer ao professor:**
   > *"O servidor anuncia os seus streams com mensagens `ANUNCIO` periódicas (de 10 em 10s, com número de sequência `seq` e métrica de atraso acumulado). Como podem ver no comando `rotas`, os nós intermédios e os clientes já sabem o melhor caminho até `S1`, mas a rota fica **`Ativa: False`** e `Destinos: []`. Isto garante que **não há tráfego de vídeo UDP na rede** enquanto nenhum cliente pedir o stream."*
   - *(Extra se quiserem comprovar no terminal de `n5` ou `S1` antes do `play`: `tcpdump -i any udp port 25000` mostra **0 pacotes**!)*

---

### 🎬 ATO 3 — Plano de Dados, 1.º Cliente e Encapsulamento RTP *(~2 min)*

1. **O que fazer:**
   - No separador do cliente **`n20`**, escrever:
     ```text
     play Stream1
     ```
2. **O que vai aparecer no ecrã:**
   - Nos terminais a montante (`n5`, `n9`, `n10`, `S1`) aparece a propagação pelo caminho inverso:
     ```text
     [*] Rota Stream1 ATIVADA por 10.0.18.20. A propagar para 10.0.12.2...
     ```
   - Abre-se a **janela gráfica do `VideoClient`** no `n20` a reproduzir o vídeo `movie.Mjpeg`, com a barra de estado em baixo a contar `Seq`, `Frames exibidos`, `Recebidos` e `Bytes`.
3. **O que dizer ao professor:**
   > *"Quando o cliente `n20` faz `play`, o pedido `ATIVA` sobe pelo caminho inverso até `S1`. Só nessa altura é que o `StreamServer` no `S1` começa a ler os frames do `.Mjpeg`, empacotá-los num cabeçalho RTP padrão de 12 bytes (`V=2`, `PT=26` JPEG, número de sequência cíclico e timestamp em ms) com o prefixo `Stream1|` e a enviá-los por UDP na porta `25000`."*
   > *"Para manter o framerate estável de 20 fps sem acumular atraso, o servidor desconta o tempo de leitura de disco e envio da rede usando `time.perf_counter()`. Além disso, quando o ficheiro `movie.Mjpeg` chega ao fim (~25s), o `VideoStream` faz `seek(0)` automático para transmitir em loop contínuo. Nos nós intermédios (`n10`, `n9`, `n5`), o `OverlayForwarder` só faz `split` no prefixo `Stream1|` para saber o fluxo e reencaminha os bytes em bruto — **sem descodificar RTP nem JPEG**, poupando CPU nos contentores."*

---

### 🎬 ATO 4 — Multicast Aplicacional (2.º Cliente) e `TRACE` *(~2 min)*

1. **O que fazer:**
   - No separador do 2.º cliente (**`n21`**), escrever:
     ```text
     play Stream1
     ```
   - Depois de abrir a 2.ª janela de vídeo (ambas a dar ao mesmo tempo!), mostrar `rotas` em **`S1`** e em **`n5`**:
     ```text
     rotas
     ```
   - Por fim, no separador de **`S1`**, executar:
     ```text
     trace Stream1
     ```
2. **O que vai aparecer no ecrã:**
   - Em **`S1` (`rotas`)**: `Destinos: ['10.0.19.1']` (**apenas 1 destino**!).
   - Em **`n5` (`rotas`)**: `Destinos: ['10.0.18.20', '10.0.18.21']` (**2 destinos**!).
   - Ao correr `trace Stream1` em `S1`, nos nós do caminho aparece:
     ```text
     [TRACE-LOG] Fluxo: Stream1 | Caminho acumulado: S1 -> n10 -> n9 -> n5 | Atraso até aqui: ... ms | Rota Ativa: True
     [TRACE-FIM] O teste de percurso para 'Stream1' chegou ao destino final (n20)! Percurso: S1 -> n10 -> n9 -> n5 -> n20
     ```
3. **O que dizer ao professor:**
   > *"Agora temos dois clientes (`n20` e `n21`) a assistir ao mesmo fluxo em simultâneo. Reparem na tabela de rotas de `S1`: o servidor continua a enviar **apenas uma única cópia** do vídeo até `n10` (`10.0.19.1`). É apenas no nó de ramificação `n5` que o `OverlayForwarder` replica o pacote UDP para `n20` e `n21` — evitando tráfego duplicado no tronco principal da rede. Com o comando `trace` confirmamos exatamente o caminho percorrido até cada cliente."*

---

### 🎬 ATO 5 — Pruning / Desativação Limpa (`DESATIVA`) *(~1 min)*

1. **O que fazer:**
   - Fechar primeiro a janela de vídeo de **`n20`** (clicando no botão **`X`**).
   - Mostrar no separador de **`n5`** que o `n21` **continua a ver o vídeo normalmente** (`rotas` em `n5` mostra agora apenas `['10.0.18.21']`).
   - Fechar agora a janela de vídeo de **`n21`** (no botão **`X`**).
   - Executar `rotas` em **`n5`** e **`S1`**.
2. **O que vai aparecer no ecrã:**
   - Em `n20` / `n21`:
     ```text
     [*] VideoClient fechado. Rota Stream1 DESATIVADA (sem destinos restantes).
     ```
   - Em `n5` e `S1`:
     ```text
     [*] Rota Stream1 DESATIVADA (sem destinos restantes).
     ```
     E em `rotas` no `S1`: `Destinos: [] | Ativa: False`.
3. **O que dizer ao professor:**
   > *"Quando o cliente `n20` fecha a janela, envia automaticamente `DESATIVA` a `n5`. Como o `n21` ainda está a assistir, `n5` remove apenas `n20` e mantém o stream ativo. Só quando o último cliente (`n21`) fecha a janela é que o pedido `DESATIVA` sobe até `S1`, passando `Ativa: False` e fazendo o `StreamServer` parar imediatamente de enviar pacotes."*

---

### 🎬 ATO 6 — Tolerância a Falhas / Re-roteamento *(se tiverem caminho alternativo no `topology.conf`)*

> **Importante:** Este passo só mostra re-roteamento alternativo quando o `config/topology.conf` já tiver **pelo menos 2 caminhos** até aos clientes (Cenário 3). Se estiverem no cenário linear de 6 nós, expliquem que é o comportamento de `limpar_rotas_vizinho_falhado` + rondas `seq` assim que houver caminho redundante.

1. **O que fazer (quando tiverem malha / Cenário 3):**
   - Com um cliente a ver o vídeo, ir ao separador de um nó intermédio da rota ativa (ex.: `n9`) e escrever `sair` (ou desligar o link no CORE).
2. **O que vai aparecer no ecrã:**
   - Nos vizinhos: `[-] Vizinho ... desligou-se (rotas associadas atualizadas).`
   - Na ronda seguinte de `ANUNCIO` (máx. 10s):
     ```text
     [RE-ROTEAMENTO] Rota para 'Stream1' transferida de <IP_antigo> para <IP_novo>!
     ```
   - O `ott.py` reenvia automaticamente `ATIVA` para a nova origem e o vídeo retoma no cliente.

---

## 3. Perguntas Clássicas do Professor (Respostas Rápidas)

| Pergunta do Professor | O que responder |
|---|---|
| **"Porque usaram TCP no controlo e UDP nos dados?"** | O controlo (`ANUNCIO`, `ATIVA`, `DESATIVA`) precisa de entrega garantida e deteção rápida de queda de vizinho por quebra da ligação TCP. O vídeo usa UDP para baixa latência sem bloqueios por retransmissão TCP (*head-of-line blocking*). |
| **"Os nós intermédios processam ou abrem a imagem JPEG?"** | Não. O formato no fio é `<stream_id>\|<RTP 12B><JPEG>`. O `OverlayForwarder` faz apenas `split(b'\|', 1)` para ler o ID do stream e reenvia os bytes brutos diretamente, sem usar Pillow nem descodificar RTP. |
| **"Porque usaram `65536` no `recvfrom` e `SO_REUSEADDR`?"** | Frames MJPEG podem passar os 20–30 KB (`movie.Mjpeg` chega a ~14 KB, mas frames maiores iam ser truncados com buffer pequeno), e `SO_REUSEADDR`/`SO_REUSEPORT` evita `Address already in use` ao reiniciar rápido no CORE. |
| **"Como evitam que a GUI do Tkinter bloqueie ou crashe?"** | A thread de escuta UDP descodifica o JPEG em memória (`BytesIO`) e envia para uma `queue.Queue(maxsize=5)` (descartando frames velhos se a GUI atrasar). A thread principal do Tkinter lê a fila via `after()` e cria o `ImageTk.PhotoImage` exclusivamente na thread da GUI. |
| **"E se houver 2 streams ao mesmo tempo no servidor?"** | O `StreamServer` lança uma thread trabalhadora independente por `stream_id`, cada pacote leva o prefixo `<stream_id>\|` e o `VideoClient` filtra só pelo seu `stream_id`. |

---

## 4. Plano B — Se Algo Correr Mal ao Vivo

- **Se o CORE ficar com processos presos ou sem janelas:**
  ```bash
  ./scripts/stop_core.sh
  xhost +local:
  ./scripts/launch_core.sh --auto
  ```
- **Se não houver ambiente gráfico / falhar o X11 no CORE:**
  - Mostrar as rotas e pacotes no CORE (`rotas`, `trace`, `tcpdump -i any udp port 25000`) e demonstrar o cliente gráfico localmente no host com:
    ```bash
    python3 scripts/demo_video_local.py
    ```
    e a suite completa com:
    ```bash
    python3 scripts/run_all_tests.py
    ```
