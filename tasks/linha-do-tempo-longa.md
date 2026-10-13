# Linha do tempo longa: a Suíça acendendo desde 1883

**Objetivo**: a animação da linha do tempo começa em 1883 e mostra cada empresa acendendo no seu prédio na data de inscrição no registro comercial, até chegar a 2018, quando entram os fechamentos e a animação atual. É a peça do vídeo da inscrição.
**Prazo**: vídeo em 14/10/2026; inscrição em 16/10/2026.
**Status**: especificação aprovada em conversa (13/10/2026); aguardando leitura do usuário.
**Branch / worktree**: `feat/cantonal-register`, em `~/Projetos/swissviz-cantonal` (já contém a `feat/timeline-shab`).
**Depende de**: `tasks/cantonal-register.md` (coleta P1 e P2), `tasks/timeline-shab.md`.

---

## Decisões do usuário (13/10/2026)

- Fazer a **linha do tempo longa** primeiro; ficha com histórico, mapa por idade e achados ficam para depois.
- **No git entra só a data de inscrição de cada empresa.** Histórico (nomes, sedes, endereços, capital, diário) e extratos brutos ficam fora do git, em `build/`.
- **Uma linha contínua desde 1883, com velocidade variável**, em vez de começar em 1980 ou de dois modos separados.
- **Entra na inscrição**, em versão enxuta.
- **Um arquivo só de lâmpadas** com a época em 1883 (abordagem A).
- **As formas jurídicas de hoje continuam valendo** para todas as lâmpadas (cores, filtros, legenda).

## O que os dados permitem (e o que não)

Antes de 2018 só conhecemos empresas que **existem hoje** (P2) ou que **fecharam em 2018–2026** (P1). Quem abriu e fechou antes de 2018 não está nos dados. O trecho antigo mostra, portanto, a Suíça de hoje sendo construída: só luzes acendendo. Fechamentos aparecem de 2018 em diante. Isso é dito na legenda.

## 1. Dados

Tudo por script, regenerável; nada editado à mão.

### 1.1 `data/registered.json.gz` (novo, entra no git)
- Gerado por `scripts/fetch_cantonal.py --parquet`, a partir do cache `build/cantonal/`.
- Formato colunar, como `opening_dates.json.gz`: `{"asOf": "AAAA-MM-DD", "uid": [123456789, …], "date": [18830309, …]}`; `uid` como inteiro de 9 dígitos, `date` como AAAAMMDD.
- Uma linha por UID com `status == "ok"` e data conhecida. Data = `registered`; se vier vazio e o extrato não tiver "report" de migração (`has_report` falso), usa a primeira entrada do diário (`first_journal`).
- UID presente em dois registros (mudança de cantão; ex.: Nestlé em ZG e VD): fica a **data mais antiga**.
- Nada além de UID e data.

### 1.2 Empresas ativas com endereço (`build/lindas_active.parquet`, fora do git)
- Estende `lindas_firms()` em `fetch_cantonal.py` (ou função irmã) para trazer, por empresa ativa: `uid`, nome (`schema:legalName`), forma jurídica (`schema:additionalType`, código eCH-0097), rua, número, CEP, localidade, cantão, município BFS.
- Geocodificação com `scripts/swisstopo_geo.py::geocode` (o mesmo da linha do tempo), que devolve `lat`, `lng` e `prec`.

### 1.3 `scripts/build_timeline.py`
- `EPOCH` passa de 2018-01-01 para **1883-01-01**. Dias em `uint16` (65535 = sem data) cobrem até 2062.
- Três fontes de lâmpadas, nesta ordem de precedência por UID:
  1. **Eventos do SHAB** (como hoje): abertura e fechamento de 2018–2026, no endereço do evento.
  2. **Canceladas sem abertura no SHAB** (as "close only" de hoje): o dia de abertura passa a ser a data de `registered.json.gz`, quando houver; senão continuam "close only".
  3. **Ativas sem nenhum evento no SHAB**: uma lâmpada nova, abertura = data de inscrição, sem fechamento, no endereço atual (1.2), com a forma jurídica do LINDAS convertida pelo mesmo `LEGAL`.
- **Sem data exata** (Friburgo enquanto a coleta não termina; não encontradas): usa a estimativa de `~/Projetos/swissviz/data/opening_dates.json.gz` (branch night-light, só leitura): precisão 1 → o dia; precisão 2 → 1º de julho do ano; precisão 3 ("antes de ~1999") ou ausente → **sem lâmpada no trecho antigo** (a empresa não aparece), contada no log.
- Datas anteriores a 1883-01-01 → dia 0.
- Abertura posterior ao fechamento (dado inconsistente) → a lâmpada vira "close only", contada no log.
- Saída: os mesmos `data/timeline/events.bin.gz` e `names.json.gz`, mesmas colunas, ordenados por abertura. Estimativa: ~1,1 milhão de lâmpadas, ~6 MB e ~11 MB.
- O log imprime: lâmpadas por fonte, quantas com data exata, estimada e sem data, e a distribuição por década.

## 2. Interface (`index.html`)

- `TL_EPOCH = Date.UTC(1883, 0, 1)`; `TL_VERSION` sobe (o service worker guarda os arquivos de dados).
- **Velocidade variável.** Hoje `tlDay += dt * TL_DAYS_PER_SEC * tlSpeed`. Passa a existir um relógio de tela `tlT` (segundos a 1×) e uma tabela `tlT → dia`, montada ao carregar a partir de `cumOpen`:
  - antes de 2018-01-01: cada segundo acende um número parecido de empresas, limitado a no máximo **8 anos por segundo** e no mínimo ~1 ano a cada 2 s; duração-alvo do trecho 1883–2018: **~45 s** a 1×;
  - de 2018 em diante: o ritmo atual (um ano em ~7 s).
  - `tlDay` continua sendo o que o shader recebe; só muda como ele avança.
- Flash de abertura e fade de fechamento (`TL_FLASH_SEC`, `TL_FADE_SEC`) são definidos em segundos de tela: o shader recebe as durações em dias calculadas com a velocidade local, para que uma abertura em 1950 e uma em 2020 brilhem pelo mesmo tempo na tela.
- **Régua e controle deslizante**: posição proporcional ao tempo de tela (não ao dia), para que arrastar seja uniforme; marcas de ano em 1900, 1950, 1980, 2000, 2010, 2018 e anuais depois. O rótulo mostra o ano antes de 2018 e a data completa depois.
- Contadores de aberturas e fechamentos e o gráfico semanal: antes de 2018 o gráfico usa caixas por ano em vez de por semana (a série semanal de 140 anos não cabe nem faz sentido).
- **Nota na legenda** (nas línguas que o site já tem): "Antes de 2018: empresas que existem hoje ou que fecharam desde 2018. As que abriram e fecharam antes não constam."
- Nada muda em cores, filtros por forma jurídica, ficha da empresa ou busca.

## 3. Desempenho

- ~1,1 milhão de instâncias em vez de ~660 mil. Medir no desktop e num celular: tempo de carga, quadros por segundo na animação e memória.
- Se o celular não segurar: no celular a linha do tempo carrega só as lâmpadas com abertura ou fechamento de 2018 em diante (corte feito no cliente, pelo dia), e a régua começa em 2018. Decisão tomada pela medição, não antes.

## 4. Verificação

- **Gerador** (teste automatizado, `scripts/test_build_timeline.py`): com um conjunto pequeno de eventos, datas e ativas, conferir a precedência das três fontes, o tratamento de data anterior a 1883, de abertura posterior ao fechamento e da estimativa por precisão; ler de volta o binário e conferir contagem e colunas.
- **Dados reais**: Nestlé (CHE-105.909.036) acende em 1883; UBS (CHE-101.329.561) em 1978; nenhuma lâmpada com fechamento antes da abertura; contagem de ativas = contagem do LINDAS menos as sem coordenada e sem data.
- **Visual**, no navegador (skill `terminal-browser`): quadros em 1900, 1950, 1995, 2018 e 2024; arrastar a régua de ponta a ponta; conferir que a transição em 2018 não tem salto.

## 5. Ordem de trabalho e corte para o vídeo

1. `registered.json.gz` + ativas geocodificadas + `build_timeline.py` com época em 1883, **com a velocidade atual** → já existe uma linha longa funcionando (lenta no começo).
2. Velocidade variável e régua.
3. Flash e fade em tempo de tela; gráfico por ano antes de 2018.
4. Nota na legenda; medição no celular.
5. Regenerar os dados quando Friburgo e a P1 terminarem.

Se o tempo acabar, o vídeo sai com o que estiver sólido nessa ordem. O passo 1 sozinho não basta para o vídeo (136 anos a um ano por 7 s são ~16 min); o mínimo para gravar é **1 + 2**.

## Fora do escopo

- Mover a lâmpada quando a empresa mudou de endereço (endereço histórico).
- Ficha da empresa com histórico; mapa por idade; achados para o texto.
- Empresas que abriram e fecharam antes de 2018.
- Publicar o histórico ou os extratos.
