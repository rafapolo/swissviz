# Cobertura dos dados: localização e data de abertura

**Objetivo**: registrar o que o projeto tem e o que falta, por grupo de empresas, em localização e em data de abertura.
**Situação em**: 13/10/2026, 13:10; atualizada em 14/10/2026 (seção "Atualização de 14/10/2026" no fim). Números medidos em `data/opening_dates.json.gz` (branch `feat/night-light`) e em `build/timeline_geo.parquet` (worktree `swissviz-timeline`).
**Relacionadas**: `tasks/cantonal-register.md` (P1 e P2), `tasks/timeline-shab.md`.

---

## Resumo

A geolocalização já existe para quase tudo. O que falta é a **data exata das ativas antigas** (P2) e, como opcional, o **endereço histórico**.

| Grupo | Quantas | Localização | Data de abertura |
|---|---|---|---|
| Ativas | 795.106 | no mapa, com lat/lng por empresa | ver tabela abaixo |
| Aberturas e cancelamentos de 2018–2026 | 661.461 eventos (410.068 aberturas, 251.393 cancelamentos) | 99,98% geocodificados no swisstopo | exata para as aberturas; a P1 traz a das canceladas |
| Canceladas antes de 2018 | — | nenhuma | nenhuma (fora do escopo; as anteriores a 2000 nem estão no Zefix) |

## Data de abertura das ativas (`opening_dates.json.gz`)

| Precisão | Quantas | Origem |
|---|---|---|
| 0 — dia exato | 342.526 | SHAB, publicação de inscrição (desde 09/2018) |
| 1 — dia interpolado | 71.865 | posição do `ehraid` entre âncoras |
| 2 — só o ano, estimado | 260.809 | posição do `ehraid` |
| 3 — "antes de ~1999" | 119.906 | `ehraid` da carga inicial, sem ordem |

As três últimas somam ~452 mil empresas. A **P2** troca essas estimativas pela data exata do registro cantonal ("Eingetragen am"). O ponto no mapa não muda, só a data.

## Precisão da geocodificação dos eventos 2018–2026 (`timeline_geo.parquet`)

| Nível | Eventos |
|---|---|
| prédio | 613.320 (92,7%) |
| CEP | 23.984 |
| rua | 12.651 |
| sede (município) | 11.407 |
| sem coordenada | 99 |

## O que cada coleta cantonal muda

- **P1** (canceladas em 2018–2026 sem abertura conhecida, ~199 mil): dá a data de abertura. Elas já têm coordenada, a do endereço no momento do cancelamento.
- **P2** (ativas inscritas antes de 09/2018): dá a data exata no lugar da estimativa. É a mais relevante para o mapa de empresas existentes (decisão do usuário em 13/10/2026: P2 antes da P1).

## O que continua faltando

- [ ] **Endereço histórico.** Os extratos trazem endereços e sedes anteriores como texto (`addresses[]`, `seats[]`, com Ei/Lö). Mover o ponto quando a empresa mudou exige geocodificar esses endereços antigos. Opcional (C4 da tarefa cantonal).
- [ ] **Ligação ponto ↔ data.** Os arquivos de pontos do mapa (`data/{cantão}_0.json.gz`) têm nome, tipo e coordenada, mas não o UID. Não foi verificado como a branch night-light liga cada ponto à sua data; conferir antes de publicar as datas da P2.
- [ ] **Canceladas antes de 2018.** Sem fonte com UID e endereço. Só o SHAB escaneado ou o arquivo em papel.

## Ressalvas das datas do registro cantonal (validação parcial, 18.537 extratos da P1)

- A data veio em 99,4% dos extratos. Em 113 casos (FR e GE) o campo veio vazio; a reserva é a primeira entrada do diário.
- 17 empresas não foram encontradas no registro (0,1%).
- Em ~0,25% a data é posterior à primeira entrada do diário: empresa que mudou de cantão e mostra a nova inscrição. Na P2, o registro de origem (pelo CHID) entra como segundo host para esses casos. Conferir com o conjunto completo.
- O casamento só por nome do arquivo SHAB 2002–2018 erra por mais de 1 ano em 10,5% dos casos; onde a data vier só dele, não é confiável.

---

## Atualização de 14/10/2026

### 1. Ano de abertura no card do mapa (`'98`)

- `scripts/canton_years.py` acrescenta a coluna `y` (ano, 0 = desconhecido) aos arquivos `data/{cantão}_{k}.json.gz`. O card mostra `'98` ao lado do nome; o ano completo fica no `title`.
- Ordem das fontes (a última vence): estimativa de `opening_dates.json.gz` com precisão 0–2 → abertura no SHAB → registro cantonal (`data/registered.json.gz`, 638.252 empresas depois da coleta de Friburgo).
- Os arquivos de cantão não têm UID. A empresa é achada no LINDAS pelo **nome + cantão**; se não der, só pelo nome, quando todas as homônimas têm o mesmo ano.
- Cobertura: **682.547 de 760.625 empresas (89,7%)**. Custo: +756 kB em gzip somando os 26 cantões (~7%).

### 2. SHAB atualizado

- Publicações de 09 e 10/2026 buscadas em 14/10/2026: +610 eventos, total **662.071** (410.437 inscrições, 251.634 cancelamentos).
- A linha do tempo foi refeita com eles: 1.049.430 lâmpadas.
- Para atualizar de novo sem sobrescrever a lista completa `build/shab_ids.parquet`, rodar `fetch_shab.py --start AAAA-MM` numa pasta temporária com `build/shab_events.jsonl` como link para o arquivo real (o script anexa e pula os ids que já tem).

### 3. Por que os pontos mudam ao ligar a linha do tempo

O mapa normal e a linha do tempo vêm de fontes diferentes:

| | Mapa normal | Linha do tempo |
|---|---|---|
| Empresas | arquivos de cantão de **04/2026** (760.625) | SHAB 2018–2026 + LINDAS de 14/10/2026 (797.894 acesas hoje) |
| Geocodificação | Mapbox, 4 casas decimais | swisstopo (registro de endereços) |
| Endereço de quem abriu desde 2018 | atual (em 04/2026) | **o da inscrição no SHAB** |
| Nome | atual (em 04/2026) | o da inscrição no SHAB |

Diferenças medidas:

- **Mapa normal × LINDAS** (674 mil pares por nome + cantão): metade a menos de 4 m, **10,4% a mais de 100 m**, 4,6% a mais de 1 km. Quase tudo vem de empresas que um dos lados só localizou pelo CEP (mediana de 513 m) ou pela sede (~1 km).
- **Lâmpada × endereço atual**: das 342.647 empresas abertas no SHAB desde 2018 e que existem hoje, **77.584 (22,6%) acendem a mais de 100 m do endereço atual** (16,9% a mais de 1 km). Elas se mudaram depois da inscrição. É a maior causa dos pontos que "pulam".
- **Nomes**: 38.405 dessas empresas mudaram de nome depois da inscrição. A lâmpada mostra o nome antigo.
- **Pontos que só existem num lado**: 81 mil empresas do mapa normal não têm par por nome + cantão no LINDAS (fecharam desde 04/2026 ou mudaram de nome). 3.194 lâmpadas acesas hoje não existem mais no LINDAS: saíram do registro sem cancelamento HR03 no SHAB (fusão, transferência etc.).

### 4. Geolocalização por fonte

| Fonte | Prédio | Rua | CEP | Sede | Sem local |
|---|---|---|---|---|---|
| SHAB 2018–2026 (662 mil eventos) | 92,7% | 1,9% | 3,6% | 1,7% | 99 eventos, fora do mapa |
| LINDAS, ativas (795 mil) | 94,7% | 1,9% | 2,9% | 0,5% | 0 |

Ressalvas:

- **Antes de 2018 o lugar é o de hoje.** A lâmpada de uma empresa aberta em 1950 fica no endereço atual dela. O histórico de endereços existe nos extratos cantonais (`addresses[]`, `seats[]`), mas não foi geocodificado.
- **Canceladas antes de 2018 não aparecem.** Só entram as que existem hoje ou foram canceladas a partir de 2018.

### Proposta para o item 3: uma fonte só para "hoje"

1. **Mapa normal a partir do LINDAS.** Gerar `data/{cantão}_{k}.json.gz` de `build/lindas_active.parquet`: nome atual, coordenada do swisstopo, ano pelo UID (sem casar por nome: 795.158 de 795.271 têm ano, 99,99%, contra 89,7% hoje). Refazer `search_index.json.gz` e os totais (`scripts/canton_counts.py`) com a mesma lista. Os arquivos de 04/2026 e o Mapbox saem do caminho.
2. **Lâmpadas acesas hoje no endereço e com o nome atuais.** No `build_timeline.py`, quem existe no LINDAS usa a coordenada e o nome de lá; quem foi cancelado fica no endereço do cancelamento. Assim, no fim da linha do tempo, cada lâmpada acesa coincide com um ponto do mapa normal.
3. **As 3.194 que sumiram sem HR03** apagam na data do snapshot do LINDAS em que deixaram de aparecer (ou na data de hoje, como aproximação), marcadas como "saída sem publicação".
4. Opcional: mostrar a mudança de endereço (a lâmpada se move na data da mudança) exige as publicações de mutação (HR02) do SHAB, que ainda não são baixadas.
