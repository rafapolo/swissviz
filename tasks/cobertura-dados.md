# Cobertura dos dados: localização e data de abertura

**Objetivo**: registrar o que o projeto tem e o que falta, por grupo de empresas, em localização e em data de abertura.
**Situação em**: 13/10/2026, 13:10. Números medidos em `data/opening_dates.json.gz` (branch `feat/night-light`) e em `build/timeline_geo.parquet` (worktree `swissviz-timeline`).
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
