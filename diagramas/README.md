# Diagramas

Cada diagrama conceitual ou de arquitetura do roteiro existe em dois formatos, com o mesmo nome:

- **`.svg`** — a imagem que aparece no README.
- **`.excalidraw`** — a cena editável. Abra em [excalidraw.com](https://excalidraw.com) (menu **Abrir**, ou arraste o arquivo para a página) para editar ou desenhar por cima durante a aula.

Para atualizar uma imagem depois de editar a cena: **Exportar imagem → SVG**, com fundo, e salve por cima do `.svg` de mesmo nome. Salve também a cena (`.excalidraw`), para as duas versões não divergirem.

| Arquivo | Seção |
|---|---|
| `01-cron-vs-orquestrador` | 1 — Por que orquestrar? (cron × Airflow) |
| `02-ambiente-do-laboratorio` | O ambiente — serviços do container e portas |
| `03-arquitetura-airflow3` | 2 — Componentes do Airflow 3 e a Task Execution API |
| `04-pipeline-pedidos` | Parte 2 — O pipeline `pedidos_mensal` e o datalake |
| `05-data-interval` | 7.a — Data interval: `CronDataIntervalTimetable` × `CronTriggerTimetable` |
| `06-ciclo-de-vida-task` | 8 — Estados de uma task instance |
| `07-assets` | 10 — Assets, Dynamic Task Mapping e branching |

## Acessibilidade

Os diagramas seguem três regras, para continuarem legíveis no GitHub (que reduz imagens largas à largura da coluna, cerca de 880 px) e para pessoas com baixa visão:

- **Largura máxima de ~1.100 px**: acima disso, o GitHub reduz a imagem e o texto encolhe junto.
- **Fonte Nunito** (no Excalidraw: *Normal*), não a manuscrita, com **no mínimo 20 px** (rótulos de 22 a 24 px, títulos de 34 px).
- **Contraste mínimo de 4,5:1** (WCAG AA) entre o texto e o fundo.

Ao editar uma cena, mantenha essas regras.
