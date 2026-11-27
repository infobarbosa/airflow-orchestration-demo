# Orquestração de Pipelines de Dados com Apache Airflow 3

Author: Prof. Barbosa<br>
Github: [infobarbosa](https://github.com/infobarbosa)

Nos laboratórios de Spark, você processou os arquivos de pedidos rodando um job de cada vez, à mão. Em produção ninguém faz isso: o arquivo de cada mês precisa ser processado **todo mês, na ordem certa, com novas tentativas quando algo falha, sem duplicar dados ao reprocessar e com um histórico de tudo o que rodou**. Esse é o trabalho de um **orquestrador**, e o Apache Airflow é o orquestrador mais usado em engenharia de dados.

Neste laboratório você constrói, passo a passo, o pipeline mensal de pedidos de um e-commerce: o Airflow extrai o arquivo do mês, submete um job **PySpark** a um cluster Spark, carrega o resultado no **PostgreSQL** e, quando a tabela é atualizada, dispara automaticamente um segundo pipeline de relatórios.

Os comandos são copy-paste no terminal do laboratório, na ordem em que aparecem.

---

## O que você vai saber fazer ao final

- Explicar o que um orquestrador resolve e que o `cron` não resolve: dependências, novas tentativas, histórico, reprocessamento e visibilidade.
- Identificar os componentes do Airflow 3 (API Server, Scheduler, DAG Processor, Triggerer, executor e banco de metadados) e o papel de cada um numa execução.
- Escrever DAGs com a **TaskFlow API** (`@dag`, `@task`) e com operadores clássicos (`BashOperator`, `SparkSubmitOperator`), passando dados entre tasks por **XCom**.
- Testar um DAG sem o scheduler (`airflow dags test`, `airflow tasks render`).
- Configurar **Connections**, **Variables** e **Pools** pela CLI.
- Explicar **logical date** e **data interval**, e a diferença entre `CronTriggerTimetable` e `CronDataIntervalTimetable` no Airflow 3.
- Processar o histórico com **backfill**, reprocessar um mês com `airflow tasks clear` e justificar por que a carga precisa ser **idempotente** e **atômica**.
- Tornar um pipeline resiliente com **retries**, **callbacks de falha** e **trigger rules**.
- Esperar a chegada de um arquivo com um **sensor deferrable** e explicar o papel do **Triggerer**.
- Encadear pipelines por dados com **Assets** e usar **Dynamic Task Mapping** e **branching**.

---

## Pré-requisitos

- Acesso ao **AWS Academy** (o ambiente é uma instância EC2 de **8 GB de RAM** criada pelo script `launch-lab.sh`, logo a seguir).
- Um navegador. Nada precisa ser instalado: o Airflow, o cluster Spark, o PostgreSQL e os dados já vêm prontos no ambiente.
- Noções de Python e de DataFrames do Spark (as do curso bastam).

Fora da aula, o mesmo ambiente roda em qualquer máquina com **Docker** — veja o [Apêndice D](#d-execução-local).

---

## Índice

[O ambiente](#o-ambiente)

**Parte 1 — Fundamentos**

1. [Por que orquestrar?](#1-por-que-orquestrar)
2. [A arquitetura do Airflow 3 no laboratório](#2-a-arquitetura-do-airflow-3-no-laboratório)
3. [O primeiro DAG](#3-o-primeiro-dag)

**Parte 2 — O pipeline de pedidos**

4. [Preparando o terreno: dados, Connections, Pool e tabela](#4-preparando-o-terreno-dados-connections-pool-e-tabela)
5. [O job PySpark](#5-o-job-pyspark)
6. [O DAG `pedidos_mensal`](#6-o-dag-pedidos_mensal)

**Parte 3 — O tempo no Airflow**

7. [Logical date, data interval, catchup e backfill](#7-logical-date-data-interval-catchup-e-backfill)

**Parte 4 — Resiliência**

8. [Retries, callbacks e trigger rules](#8-retries-callbacks-e-trigger-rules)
9. [Sensores deferrable e o Triggerer](#9-sensores-deferrable-e-o-triggerer)

**Parte 5 — Pipelines encadeados por dados**

10. [Assets, Dynamic Task Mapping e branching](#10-assets-dynamic-task-mapping-e-branching)
11. [Boas práticas](#11-boas-práticas)

**Apêndices**

- [A. Referência rápida de comandos](#a-referência-rápida-de-comandos)
- [B. Encerrando o ambiente](#b-encerrando-o-ambiente)
- [C. Troubleshooting](#c-troubleshooting)
- [D. Execução local](#d-execução-local)

---

## O ambiente

O laboratório roda em uma instância **AWS EC2 com 8 GB de RAM** (`t3.large`) criada a partir do **AWS CloudShell**. A instância executa um container com o **code-server** (Visual Studio Code no navegador), o **Apache Airflow 3.4** com o banco de metadados no **PostgreSQL 16** e um **cluster Apache Spark 4.3** (1 master e 1 worker).

![Os serviços do laboratório dentro do container: code-server, os quatro componentes do Airflow, PostgreSQL e o cluster Spark](diagramas/02-ambiente-do-laboratorio.svg)

> **ATENÇÃO:**
> 1. **Região:** utilize sempre a região **Norte da Virgínia (`us-east-1`)** no Console AWS.
> 2. **Custos:** a instância EC2 e o volume EBS consomem créditos do seu laboratório AWS Academy. Ao encerrar a aula, garanta que a instância esteja parada (**Stopped**) ou aguarde o desligamento automático da sessão.
> 3. **Não recrie a máquina para continuar o trabalho:** cada execução do `launch-lab.sh` cria uma instância **nova e vazia**, com data e hora no nome. Se a sua instância já existir, siga as instruções de [Retomando o laboratório](#retomando-o-laboratório).

### Passo 1: Abrir o AWS CloudShell

1. Faça login no Console da AWS através do seu painel do AWS Academy.
2. Certifique-se de que a região selecionada no canto superior direito é **N. Virginia (`us-east-1`)**.
3. No topo do console, clique no ícone do **AWS CloudShell** (`>_`) ao lado da barra de pesquisa.
4. Aguarde o terminal do CloudShell carregar.

### Passo 2: Executar o script de provisionamento

No terminal do CloudShell, execute:

```sh
curl -sS https://raw.githubusercontent.com/infobarbosa/airflow-orchestration-demo/main/launch-lab.sh | bash

```

O script realiza de forma automatizada:

- Descoberta da VPC padrão e de uma Subnet numa zona que ofereça o tipo `t3.large` (8 GB de RAM).
- Configuração do Security Group `lab-airflow-sg`, liberando as portas `8080` (IDE), `8081` (Airflow), `9090` e `8082` (Spark) e `22` (SSH).
- Lançamento de uma nova instância EC2 `lab-airflow-orchestration-AAAA-MM-DD_HH_MI_SS` (Ubuntu 24.04, 20 GB gp3).
- Instalação do Docker e inicialização do container `airflow-lab`.
- Exibição do IP público e dos links de acesso.

### Passo 3: Acessar a IDE

1. Abra no navegador o link exibido ao final do script: `http://<IP-PUBLICO>:8080`.
2. Caso a página não responda, **aguarde de 3 a 5 minutos** para que o Docker conclua o download da imagem e atualize o navegador.
3. O code-server abre diretamente na IDE, sem solicitar senha.
4. Abra um terminal: menu superior (três linhas horizontais) > **Terminal** > **New Terminal**.

![Menu do code-server aberto: ícone de três linhas no canto superior esquerdo, item Terminal e, no submenu, New Terminal](images/ide-01-menu-terminal.png)

O terminal abre na parte de baixo da IDE. Teste com o comando `svc-status`, que lista os serviços do laboratório:

![Terminal do code-server na parte inferior da tela, com a saída do comando svc-status mostrando os sete serviços no estado UP](images/ide-02-terminal-svc-status.png)

> **Atenção!** Todos os comandos deste laboratório são executados no **terminal do code-server**. O CloudShell só serve para criar a instância.

As outras interfaces web ficam no mesmo IP:

| Interface | Endereço | Para quê |
|---|---|---|
| **IDE (code-server)** | `http://<IP-PUBLICO>:8080` | Editar arquivos e rodar os comandos do roteiro |
| **Airflow** | `http://<IP-PUBLICO>:8081` | Acompanhar DAGs, execuções, tasks e logs (sem login) |
| **Spark Master** | `http://<IP-PUBLICO>:9090` | Ver o worker e os jobs submetidos pelo Airflow |

### Retomando o laboratório

Se a sessão do AWS Academy expirar, a instância será parada. **Não execute o `launch-lab.sh` novamente:** ele criaria uma instância nova, sem o seu trabalho.

1. No Console AWS (`us-east-1`), acesse **EC2** > **Instances**.
2. Selecione a sua instância **`lab-airflow-orchestration-<data e hora>`** (se houver mais de uma, a mais recente) e clique em **Instance state** > **Start instance**.
3. Aguarde o status **Running** e copie o novo **Public IPv4 address** (ele muda a cada reinício).
4. Acesse `http://<NOVO_IP_PUBLICO>:8080`. O container reinicia automaticamente; os seus DAGs, as execuções registradas e as tabelas do PostgreSQL são preservados.

### O que acontece na subida do container

O banco de metadados do Airflow já vem criado e migrado dentro da imagem. Na subida, o container só precisa iniciar os serviços, o que leva cerca de **10 segundos**. Acompanhe pelo log de inicialização:

```sh
# 0.1
cat /var/log/lab/boot.log

```

Output esperado:
```
[boot] 2026-11-27 12:22:56 Iniciando os servicos do laboratorio...
[postgres] Iniciado (PID 60, log: /var/log/lab/postgres.log).
[spark-master] Iniciado (PID 94, log: /var/log/lab/spark-master.log).
[spark-worker] Iniciado (PID 230, log: /var/log/lab/spark-worker.log).
[api-server] Iniciado (PID 352, log: /var/log/lab/api-server.log).
[scheduler] Iniciado (PID 440, log: /var/log/lab/scheduler.log).
[dag-processor] Iniciado (PID 479, log: /var/log/lab/dag-processor.log).
[triggerer] Iniciado (PID 521, log: /var/log/lab/triggerer.log).
[boot] Airflow API Server respondendo em http://localhost:8081
SERVICO        ESTADO PID      PORTA
-------        ------ ---      -----
postgres       UP     60       5432
spark-master   UP     94       7077 (UI 9090)
spark-worker   UP     230      UI 8082
api-server     UP     352      8081
scheduler      UP     440      -
dag-processor  UP     479      -
triggerer      UP     521      -
[boot] 2026-11-27 12:23:13 Ambiente pronto.
```

Se o arquivo ainda não tiver chegado em `Ambiente pronto.`, aguarde alguns segundos e repita o comando.

### Verificando o ambiente

Confira se tudo está no lugar:

```sh
# 0.2
curl -sS https://raw.githubusercontent.com/infobarbosa/airflow-orchestration-demo/main/scripts/check.sh | bash

```

Output esperado:
```
Memória da Máquina
  ✓ Memória RAM detectada: 7816 MB

Ferramentas
  ✓ airflow
  ✓ spark-submit
  ✓ psql
  ✓ jq
  ✓ tree
  ✓ svc-status
  ✓ aguardar-dag

Serviços
  ✓ postgres: UP
  ✓ spark-master: UP
  ✓ spark-worker: UP
  ✓ api-server: UP
  ✓ scheduler: UP
  ✓ dag-processor: UP
  ✓ triggerer: UP

Airflow
  ✓ Airflow versão 3.4.0
  ✓ Executor: LocalExecutor
  ✓ metadatabase: healthy
  ✓ scheduler: healthy
  ✓ dag_processor: healthy
  ✓ triggerer: healthy

Spark
  ✓ Spark Master com 1 worker(s) ativo(s), 2 cores
  ✓ Spark version 4.3.0

Dados de origem
  ✓ 25 arquivos mensais de pedidos (2024-01 a 2026-01)
  ✓ banco ecommerce acessível

Resultado
  Ambiente pronto para o laboratório.
```

### Convenções

Cada bloco de comando do roteiro tem na primeira linha um rótulo, como `# 3.b.1` (seção 3, subseção 3.b, primeiro bloco). Os blocos sem rótulo, logo depois, mostram a saída esperada. Trechos longos de log foram resumidos com `[...]`.

Alguns valores **mudam a cada execução**: datas e horas de execução, identificadores de execução (`manual__2026-...`), PIDs e durações. Os seus vão diferir dos do texto, e isso é esperado. Já os nomes de DAGs e tasks, os estados finais (`success`, `failed`) e os números calculados a partir dos dados (pedidos, faturamento), esses sim, devem bater.

O comando `aguardar-dag <dag_id>` é um atalho deste laboratório: ele espera o Airflow registrar o DAG e terminar as execuções em andamento e, no fim, mostra o estado de cada uma. É a mesma informação que aparece na UI do Airflow e em `airflow dags list-runs <dag_id>`. Enquanto ele espera, vale acompanhar a execução na UI.

Os comandos funcionam a partir de qualquer pasta, mas os arquivos são criados em `~/project` (a pasta aberta na IDE). Algumas seções guardam o identificador de uma execução na variável `$RUN_ID`; se você abrir um novo terminal no meio de uma seção, rode de novo o bloco que a define.

---

# Parte 1 — Fundamentos

## 1. Por que orquestrar?

Imagine o pipeline mensal de pedidos agendado no `cron` de um servidor:

```
0 2 1 * *  /opt/pipelines/pedidos.sh
```

Funciona até o primeiro problema. Se o job Spark falhar às 2h, ninguém fica sabendo até alguém reclamar do relatório. Se o arquivo do mês atrasar, o script roda sobre dados incompletos. Se for preciso reprocessar os últimos seis meses, alguém edita o script e roda à mão, mês a mês. E se o servidor ficar desligado no dia 1º, aquele mês simplesmente não roda.

Um **orquestrador** trata o pipeline como um conjunto de **tarefas com dependências** e guarda o estado de cada execução num banco de dados. A partir daí ele consegue:

| Necessidade | `cron` | Orquestrador |
|---|---|---|
| Ordem entre etapas (extrair → transformar → carregar) | Um script único; se falhar no meio, recomeça do zero | Dependências explícitas; retoma da etapa que falhou |
| Falha transitória (rede, API fora do ar) | Falha e pronto | Novas tentativas automáticas (*retries*) |
| Saber o que rodou, quando e com qual resultado | Logs espalhados, quando existem | Histórico de cada execução e de cada tarefa, com logs |
| Reprocessar um período | Manual | *Backfill* e *clear* por intervalo de datas |
| Esperar um arquivo ou outro pipeline | `sleep` e torcida | Sensores e gatilhos por dados (*Assets*) |
| Alertar em caso de falha | Não | *Callbacks*, e-mail, Slack etc. |

![Comparação entre um script agendado no cron e o mesmo pipeline como um DAG no Airflow, com retries, histórico e reprocessamento](diagramas/01-cron-vs-orquestrador.svg)

No Airflow, o pipeline é um **DAG** (*Directed Acyclic Graph*): um arquivo Python que declara as **tasks** e as dependências entre elas. Cada execução do DAG é um **DAG run** e cada execução de uma task dentro dele é uma **task instance**. O Airflow **não processa dados**: ele decide *quando* e *em que ordem* cada task roda, e as tasks é que fazem o trabalho (um comando bash, uma consulta SQL, um job Spark).

| Termo | O que é | No pipeline de pedidos |
|---|---|---|
| **DAG** | O pipeline: tasks + dependências + agenda | `pedidos_mensal` |
| **Task** | Uma etapa do DAG | `extrair`, `transformar`, `carregar` |
| **Operator** | O "tipo" da task (o que ela sabe fazer) | `BashOperator`, `SparkSubmitOperator`, `@task` (Python) |
| **DAG run** | Uma execução do DAG | O processamento de março de 2024 |
| **Task instance** | Uma execução de uma task num DAG run | `transformar` de março de 2024, tentativa 1 |

## 2. A arquitetura do Airflow 3 no laboratório

O Airflow não é um programa único: são vários serviços que conversam por meio do **banco de metadados**.

![Arquitetura do Airflow 3: DAG Processor lê os arquivos, Scheduler decide o que rodar, executor dispara as tasks, que falam com o API Server pela Task Execution API; tudo registrado no banco de metadados](diagramas/03-arquitetura-airflow3.svg)

| Componente | Papel |
|---|---|
| **DAG Processor** | Lê os arquivos `.py` da pasta de DAGs, executa o código de topo de cada um e grava a versão serializada do DAG no banco |
| **Scheduler** | Decide quais DAG runs e task instances criar e quando cada task está pronta para rodar; entrega as prontas ao **executor** |
| **Executor** | Define *onde* as tasks rodam. Aqui é o `LocalExecutor`: subprocessos na mesma máquina. Em produção é comum o `CeleryExecutor` ou o `KubernetesExecutor` |
| **API Server** | Serve a UI e a API REST e, novidade do Airflow 3, a **Task Execution API**: as tasks não acessam mais o banco diretamente, falam com o API Server |
| **Triggerer** | Acompanha tasks *deferred* (sensores e operadores assíncronos), sem ocupar um slot de execução (seção 9) |
| **Banco de metadados** | PostgreSQL com o estado de tudo: DAGs, execuções, tasks, Connections, Variables, XComs |

### 2.a. Os serviços do laboratório

Neste ambiente, cada componente é um processo dentro do container. O comando `svc-status` mostra o estado de cada um:

```sh
# 2.a.1
svc-status

```

Output esperado:
```
SERVICO        ESTADO PID      PORTA
-------        ------ ---      -----
postgres       UP     60       5432
spark-master   UP     94       7077 (UI 9090)
spark-worker   UP     230      UI 8082
api-server     UP     352      8081
scheduler      UP     440      -
dag-processor  UP     479      -
triggerer      UP     521      -
```

Os logs de cada serviço ficam em `/var/log/lab/<serviço>.log`. Os comandos `svc-stop`, `svc-start` e `svc-restart <serviço>` param e iniciam cada um (você vai precisar deles na seção 9).

### 2.b. A configuração: `airflow.cfg`

A configuração do Airflow fica em `$AIRFLOW_HOME/airflow.cfg`. Neste laboratório o arquivo contém só o que difere do padrão:

```sh
# 2.b.1
grep -vE '^\s*(#|$)' $AIRFLOW_HOME/airflow.cfg | sed -E 's/^(fernet_key|jwt_secret) = .*/\1 = (gerado no primeiro boot)/'

```

Output esperado:
```
[core]
dags_folder = /home/barbosa/project/dags
executor = LocalExecutor
parallelism = 8
load_examples = False
dags_are_paused_at_creation = True
execution_api_server_url = http://localhost:8081/execution/
simple_auth_manager_all_admins = True
fernet_key = (gerado no primeiro boot)
[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
[api]
host = 0.0.0.0
port = 8081
workers = 1
[api_auth]
jwt_secret = (gerado no primeiro boot)
[dag_processor]
refresh_interval = 10
```

Qualquer opção também pode ser definida por variável de ambiente no formato `AIRFLOW__<SEÇÃO>__<CHAVE>` (por exemplo, `AIRFLOW__CORE__EXECUTOR`), que tem precedência sobre o arquivo. Para consultar o valor efetivo de uma opção, seja qual for a origem:

```sh
# 2.b.2
airflow config get-value core executor
airflow config get-value dag_processor refresh_interval

```

Output esperado:
```
LocalExecutor
10
```

### 2.c. A saúde dos componentes

O API Server expõe um endpoint de saúde que consulta o banco de metadados e o último *heartbeat* (sinal de vida) de cada componente:

```sh
# 2.c.1
curl -s http://localhost:8081/api/v2/monitor/health | jq '{metadatabase: .metadatabase.status, scheduler: .scheduler.status, dag_processor: .dag_processor.status, triggerer: .triggerer.status}'

```

Output esperado:
```
{
  "metadatabase": "healthy",
  "scheduler": "healthy",
  "dag_processor": "healthy",
  "triggerer": "healthy"
}
```

Abra agora a UI do Airflow em `http://<IP-PUBLICO>:8081`. A lista de DAGs está vazia: vamos criar o primeiro.

## 3. O primeiro DAG

### 3.a. Escrevendo o DAG com a TaskFlow API

Um DAG é um arquivo Python na pasta de DAGs (`~/project/dags`). Com a **TaskFlow API**, uma função decorada com `@dag` é o pipeline e cada função decorada com `@task` é uma task:

```sh
# 3.a.1
cat > ~/project/dags/ola_airflow.py <<'EOF'
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule=None,                     # sem agenda: so roda quando disparado
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab"],
)
def ola_airflow():

    @task
    def dizer_ola():
        print("Ola do Airflow!")
        return "Airflow"               # o retorno vira um XCom

    @task
    def cumprimentar(nome: str):
        print(f"{nome} cumprimenta o Spark!")

    # chamar uma task com o resultado de outra cria a dependencia dizer_ola >> cumprimentar
    cumprimentar(dizer_ola())


ola_airflow()
EOF

```

Repare em três detalhes:
- O DAG só existe para o Airflow porque a função `ola_airflow()` é **chamada** na última linha.
- `schedule=None` faz o DAG rodar apenas quando alguém o dispara.
- O valor retornado por `dizer_ola` chega a `cumprimentar` por um **XCom** (*cross-communication*): o Airflow grava o retorno no banco e o entrega à task seguinte, que pode rodar em outro processo ou até em outra máquina.

### 3.b. O DAG Processor encontra o arquivo

O DAG Processor procura arquivos novos a cada 10 segundos neste laboratório (o padrão do Airflow é 300). Espere o DAG ser registrado e liste os DAGs:

```sh
# 3.b.1
aguardar-dag ola_airflow
airflow dags list

```

Output esperado:
```
[aguardar-dag] DAG ola_airflow registrado.
[aguardar-dag] Nenhuma execucao de ola_airflow ate agora.
dag_id      | fileloc                                   | owners  | is_paused | bundle_name | bundle_version
============+===========================================+=========+===========+=============+===============
ola_airflow | /home/barbosa/project/dags/ola_airflow.py | airflow | True      | dags-folder | None
```

Se o arquivo tiver um erro de Python, o DAG não aparece na lista. O erro fica registrado e pode ser consultado com:

```sh
# 3.b.2
airflow dags list-import-errors

```

Output esperado:
```
No data found
```

### 3.c. Testando sem o scheduler: `airflow dags test`

Antes de agendar qualquer coisa, teste o DAG inteiro num único processo, sem scheduler e sem executor. É o equivalente a rodar um script localmente:

```sh
# 3.c.1
airflow dags test ola_airflow 2>&1 | grep -E '^Ola do|cumprimenta o Spark|DagRun Finished' | sed -E 's/(DagRun Finished: dag_id=[a-z_]+).*(state=[a-z]+).*/\1 ... \2/'

```

Output esperado:
```
Ola do Airflow!
Airflow cumprimenta o Spark!
2026-11-27T12:23:46.298708Z [info     ] DagRun Finished: dag_id=ola_airflow ... state=success
```

O `grep` filtra o que as tasks imprimiram e a linha final do log, que o `sed` encurta; rode só `airflow dags test ola_airflow` para ver o log completo.

### 3.d. Execução de verdade: despausar e disparar

Todo DAG novo nasce **pausado** (`dags_are_paused_at_creation = True`): o scheduler ignora DAGs pausados. Despause e dispare uma execução manual:

```sh
# 3.d.1
airflow dags unpause ola_airflow >/dev/null
airflow dags trigger ola_airflow >/dev/null
aguardar-dag ola_airflow

```

Output esperado:
```
[aguardar-dag] DAG ola_airflow registrado.
[aguardar-dag] Execucoes de ola_airflow:
  manual__2026-11-27T12:23:42.086062+00:00  success
  manual__2026-11-27T12:23:51.212677+00:00  success
```

A primeira execução da lista é a do `airflow dags test`, que também fica registrada no banco. A segunda percorreu o caminho de produção: o `trigger` criou o DAG run no banco, o **scheduler** viu as tasks prontas e as entregou ao **executor**, que as rodou em subprocessos.

Abra a UI do Airflow (`http://<IP-PUBLICO>:8081`). O menu **Dags**, na barra lateral esquerda, lista os DAGs com a última execução e o histórico recente de cada um (a sua lista terá só o `ola_airflow` por enquanto; a tela abaixo foi capturada no fim do laboratório):

![Lista de DAGs na UI do Airflow 3: cada cartão mostra o nome do DAG, as tags, o agendamento, a última execução e um gráfico de barras com as execuções recentes; à direita, o botão de pausa e o botão de disparo](images/airflow-01-lista-de-dags.png)

Clique em `ola_airflow` e depois na execução mais recente. No alto do painel esquerdo, três botões alternam entre as visões **Grade** (*Grid*: cada coluna é um DAG run e cada quadrado, uma task instance; verde = `success`), **Gantt** e **Gráfico** (*Graph*: o grafo de dependências). A tecla `g` também alterna entre Grade e Gráfico:

![Visão Gráfico do DAG ola_airflow: as tasks dizer_ola e cumprimentar ligadas por uma aresta, ambas em success; à direita, os detalhes da execução, com tipo Manual e duração](images/airflow-02-ola-airflow-graph.png)

Clique na task `cumprimentar` para abrir a task instance. A aba **Logs** mostra o log da tentativa. A saída do `print` aparece numa linha `INFO`; se os grupos estiverem fechados (▶ *Pre Execute*, ▶ *Post Execute*), clique neles para abrir:

![Aba Logs da task instance cumprimentar: grupos Log message source details, Pre Execute e Post Execute abertos, e a linha 5 com a mensagem Airflow cumprimenta o Spark!](images/airflow-03-logs-da-task.png)

Na task `dizer_ola`, a aba **XCom** mostra o valor devolvido pela função, guardado com a chave `return_value`:

![Aba XCom da task instance dizer_ola: uma entrada com a chave return_value e o valor Airflow](images/airflow-04-xcom.png)

### 3.e. Estados e logs pela CLI

O estado de cada task instance de uma execução:

```sh
# 3.e.1
RUN_ID=$(airflow dags list-runs ola_airflow -o json | jq -r '.[0].run_id')
airflow tasks states-for-dag-run ola_airflow "$RUN_ID" -o json | jq -r '.[] | "\(.task_id): \(if (.state // "") == "" then "sem estado" else .state end)"'

```

Output esperado:
```
dizer_ola: success
cumprimentar: success
```

Os logs das tasks ficam em `$AIRFLOW_HOME/logs`, um arquivo por tentativa (`attempt=1.log`), organizados por DAG, execução e task:

```sh
# 3.e.2
tree $AIRFLOW_HOME/logs/dag_id=ola_airflow/run_id=$RUN_ID

```

Output esperado:
```
/home/barbosa/airflow/logs/dag_id=ola_airflow/run_id=manual__2026-11-27T12:23:51.212677+00:00
├── task_id=cumprimentar
│   └── attempt=1.log
└── task_id=dizer_ola
    └── attempt=1.log

3 directories, 2 files
```

No Airflow 3 cada linha do log é um objeto JSON. Para ver só o que a task imprimiu:

```sh
# 3.e.3
jq -r 'select(.logger == "task.stdout") | .event' $AIRFLOW_HOME/logs/dag_id=ola_airflow/run_id=$RUN_ID/task_id=cumprimentar/attempt=1.log

```

Output esperado:
```
Airflow cumprimenta o Spark!
```

---

# Parte 2 — O pipeline de pedidos

O pipeline que vamos construir processa, a cada mês, o arquivo de pedidos daquele mês:

![Pipeline pedidos_mensal: extrair copia o arquivo do mês para a camada raw, transformar agrega com Spark e grava Parquet na camada curated, carregar grava na tabela vendas_uf do PostgreSQL e atualiza o Asset que dispara o relatorio_vendas](diagramas/04-pipeline-pedidos.svg)

| Task | Operador | O que faz |
|---|---|---|
| `extrair` | `BashOperator` | Copia `pedidos-AAAA-MM.csv.gz` do sistema de origem (`/opt/lab-dados/pedidos`) para a camada *raw* do datalake |
| `transformar` | `SparkSubmitOperator` | Submete ao cluster Spark um job PySpark que agrega os pedidos do mês por UF e grava Parquet na camada *curated* |
| `carregar` | `@task` (Python) | Lê o Parquet e grava o resultado na tabela `vendas_uf` do PostgreSQL |

## 4. Preparando o terreno: dados, Connections, Pool e tabela

### 4.a. Os dados de origem

São 25 arquivos mensais de pedidos, de janeiro de 2024 a janeiro de 2026 (os mesmos do curso de Spark):

```sh
# 4.a.1
ls /opt/lab-dados/pedidos | head -3
ls /opt/lab-dados/pedidos/*.csv.gz | wc -l
zcat /opt/lab-dados/pedidos/pedidos-2024-01.csv.gz | head -4

```

Output esperado:
```
LICENSE
pedidos-2024-01.csv.gz
pedidos-2024-02.csv.gz
25
ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE
452e8f9f-54d6-48fa-95a1-0d50a9568730;LIQUIDIFICADOR;300.0;1;2024-01-22T15:44:45;AL;7156
53d6cc71-961f-446f-b545-d12ac83bf478;MONITOR;600.0;1;2024-01-18T06:22:15;MG;4522
30dcfd9e-9883-488a-8598-c7d2d918adc4;TABLET;1100.0;1;2024-01-15T19:13:50;MA;373
```

### 4.b. Connections

Credenciais e endereços de sistemas externos não vão no código do DAG: ficam em **Connections**, guardadas no banco de metadados (as senhas criptografadas com a `fernet_key`). O DAG só se refere ao `conn_id`. Crie as três que o laboratório usa:

```sh
# 4.b.1
airflow connections add spark_default --conn-type spark --conn-host spark://localhost --conn-port 7077
airflow connections add postgres_ecommerce --conn-uri postgresql://barbosa@localhost:5432/ecommerce
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'

```

Output esperado:
```
Successfully added `conn_id`=spark_default
conn_id       | conn_type | host              | login | port | extra
==============+===========+===================+=======+======+======
spark_default | spark     | spark://localhost | None  | 7077 | {}

Successfully added `conn_id`=postgres_ecommerce
conn_id            | conn_type | host      | login   | port | extra
===================+===========+===========+=========+======+======
postgres_ecommerce | postgres  | localhost | barbosa | 5432 | {}

Successfully added `conn_id`=fs_default
2026-11-27T12:24:14.945365Z [warning  ] Skipping masking for a secret as it's too short (<5 chars) [airflow._shared.secrets_masker.secrets_masker] loc=secrets_masker.py:619
conn_id    | conn_type | host | login | port | extra
===========+===========+======+=======+======+==============
fs_default | fs        | None | None  | None | {'path': '/'}
```

| `conn_id` | Usada por | Aponta para |
|---|---|---|
| `spark_default` | `SparkSubmitOperator` (seção 6) | O Spark Master do laboratório (`spark://localhost:7077`) |
| `postgres_ecommerce` | `PostgresHook` (seção 6) | O banco `ecommerce` no PostgreSQL local |
| `fs_default` | `FileSensor` (seção 9) | O sistema de arquivos local |

```sh
# 4.b.2
airflow connections list -o table

```

Output esperado:
```
conn_id            | conn_type
===================+==========
spark_default      | spark
postgres_ecommerce | postgres
fs_default         | fs
```

As mesmas Connections aparecem na UI em **Admin > Connections**.

### 4.c. Um Pool para o cluster Spark

O cluster Spark do laboratório tem 2 cores. Se vários meses forem processados ao mesmo tempo, os jobs disputam esses cores e todos ficam lentos. Um **Pool** limita quantas tasks rodam ao mesmo tempo sobre um recurso compartilhado, seja qual for o DAG:

```sh
# 4.c.1
airflow pools set spark 1 "Jobs submetidos ao cluster Spark do laboratorio"
airflow pools list

```

Output esperado:
```
Pool spark created
pool         | slots | description                                     | include_deferred | team_name
=============+=======+=================================================+==================+==========
default_pool | 128   | Default pool                                    | False            | None
spark        | 1     | Jobs submetidos ao cluster Spark do laboratorio | False            | None
```

O `default_pool` é onde caem as tasks sem pool definido.

### 4.d. A tabela de destino

A tabela é criada uma única vez, fora do pipeline:

```sh
# 4.d.1
psql -d ecommerce -c "
CREATE TABLE vendas_uf (
    ano_mes     char(7)       NOT NULL,
    uf          char(2)       NOT NULL,
    pedidos     int           NOT NULL,
    clientes    int           NOT NULL,
    faturamento numeric(14,2) NOT NULL,
    PRIMARY KEY (ano_mes, uf)
);"

```

Output esperado:
```
CREATE TABLE
```

## 5. O job PySpark

### 5.a. O código

O job recebe o mês (`AAAA-MM`) como argumento, lê o arquivo da camada *raw* e grava o agregado por UF, em Parquet, na camada *curated*:

```sh
# 5.a.1
cat > ~/project/jobs/vendas_mensais.py <<'EOF'
"""Agrega os pedidos de um mes por UF. Uso: spark-submit vendas_mensais.py AAAA-MM"""
import sys

from pyspark.sql import SparkSession, functions as F

ano_mes = sys.argv[1]
DATALAKE = "/home/barbosa/project/datalake"

spark = (SparkSession.builder
         .appName(f"vendas_mensais_{ano_mes}")
         # sobrescreve so as particoes presentes no DataFrame (o mes processado)
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

pedidos = (spark.read
           .option("header", True)
           .option("sep", ";")
           .option("inferSchema", True)
           .csv(f"{DATALAKE}/raw/pedidos/ano_mes={ano_mes}"))

vendas_uf = (pedidos
             .withColumn("valor_total", F.col("VALOR_UNITARIO") * F.col("QUANTIDADE"))
             .groupBy(F.col("UF").alias("uf"))
             .agg(F.count("*").alias("pedidos"),
                  F.countDistinct("ID_CLIENTE").alias("clientes"),
                  F.round(F.sum("valor_total"), 2).alias("faturamento"))
             .withColumn("ano_mes", F.lit(ano_mes)))

(vendas_uf.coalesce(1)
 .write.mode("overwrite")
 .partitionBy("ano_mes")
 .parquet(f"{DATALAKE}/curated/vendas_uf"))

vendas_uf.orderBy(F.desc("faturamento")).show(3)
print(f"[vendas_mensais] {ano_mes}: {pedidos.count()} pedidos em {vendas_uf.count()} UFs")
spark.stop()
EOF

```

Dois detalhes importam para o orquestrador:
- **O mês vem de fora.** O job não calcula "o mês passado" sozinho: quem diz qual mês processar é o Airflow. Assim o mesmo job serve para a execução mensal e para reprocessar qualquer mês do histórico.
- **`partitionBy("ano_mes")` com `partitionOverwriteMode=dynamic`.** Rodar o job duas vezes para o mesmo mês **substitui** a partição `ano_mes=AAAA-MM`, sem duplicar dados nem apagar os outros meses. O job é **idempotente**.

### 5.b. Teste manual, sem o Airflow

Antes de entregar o job ao orquestrador, teste-o direto com o `spark-submit`. Primeiro, faça à mão o papel da futura task `extrair`:

```sh
# 5.b.1
mkdir -p ~/project/datalake/raw/pedidos/ano_mes=2025-12
cp /opt/lab-dados/pedidos/pedidos-2025-12.csv.gz ~/project/datalake/raw/pedidos/ano_mes=2025-12/
spark-submit --master spark://localhost:7077 ~/project/jobs/vendas_mensais.py 2025-12 2>/dev/null

```

Output esperado:
```
+---+-------+--------+-----------+-------+
| uf|pedidos|clientes|faturamento|ano_mes|
+---+-------+--------+-----------+-------+
| RJ|    352|     350|   505200.0|2025-12|
| SP|    376|     369|   469900.0|2025-12|
| MG|    347|     344|   464750.0|2025-12|
+---+-------+--------+-----------+-------+
only showing top 3 rows
[vendas_mensais] 2025-12: 3600 pedidos em 20 UFs
```

O `2>/dev/null` esconde o log do Spark (que vai para a saída de erro). O datalake ficou assim:

```sh
# 5.b.2
tree ~/project/datalake

```

Output esperado:
```
/home/barbosa/project/datalake
├── curated
│   └── vendas_uf
│       └── ano_mes=2025-12
│           └── part-00000-04ab99eb-11f7-44a9-8714-e83572c431e4.c000.snappy.parquet
└── raw
    └── pedidos
        └── ano_mes=2025-12
            └── pedidos-2025-12.csv.gz

7 directories, 2 files
```

Na UI do Spark Master (`http://<IP-PUBLICO>:9090`), o job `vendas_mensais_2025-12` aparece em **Completed Applications**.

## 6. O DAG `pedidos_mensal`

### 6.a. O código

```sh
# 6.a.1
cat > ~/project/dags/pedidos_mensal.py <<'EOF'
"""Pipeline mensal de pedidos: extrai o arquivo do mes, agrega com Spark e carrega no PostgreSQL."""
from datetime import datetime, timedelta

import pyarrow.parquet as pq

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

DATALAKE = "/home/barbosa/project/datalake"
VENDAS_UF = Asset("ecommerce.vendas_uf")

# Template Jinja: resolvido pelo Airflow na hora de rodar cada task (ex.: "2024-03")
ANO_MES = "{{ data_interval_start.strftime('%Y-%m') }}"


@dag(
    # todo dia 1 a 00:00 UTC, processando o mes que acabou de terminar (secao 7)
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2026, 1, 31),    # o ultimo arquivo disponivel e o de jan/2026
    catchup=False,
    max_active_runs=2,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["ecommerce"],
)
def pedidos_mensal():

    extrair = BashOperator(
        task_id="extrair",
        bash_command=(
            f"mkdir -p {DATALAKE}/raw/pedidos/ano_mes={ANO_MES} && "
            f"cp /opt/lab-dados/pedidos/pedidos-{ANO_MES}.csv.gz {DATALAKE}/raw/pedidos/ano_mes={ANO_MES}/ && "
            f"ls -l {DATALAKE}/raw/pedidos/ano_mes={ANO_MES}"
        ),
    )

    transformar = SparkSubmitOperator(
        task_id="transformar",
        conn_id="spark_default",
        application="/home/barbosa/project/jobs/vendas_mensais.py",
        application_args=[ANO_MES],
        name="vendas_mensais",
        pool="spark",                  # no maximo 1 job Spark por vez (secao 4.c)
    )

    @task(outlets=[VENDAS_UF])
    def carregar(data_interval_start=None, outlet_events=None):
        ano_mes = data_interval_start.strftime("%Y-%m")
        linhas = pq.read_table(f"{DATALAKE}/curated/vendas_uf/ano_mes={ano_mes}").to_pylist()

        hook = PostgresHook(postgres_conn_id="postgres_ecommerce")
        # DELETE + INSERT na mesma transacao: reprocessar o mes substitui os dados (idempotente)
        # e uma falha no meio nao deixa o mes pela metade (atomico)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM vendas_uf WHERE ano_mes = %s", (ano_mes,))
            cur.executemany(
                "INSERT INTO vendas_uf (ano_mes, uf, pedidos, clientes, faturamento) VALUES (%s, %s, %s, %s, %s)",
                [(ano_mes, l["uf"], l["pedidos"], l["clientes"], l["faturamento"]) for l in linhas],
            )

        outlet_events[VENDAS_UF].extra = {"ano_mes": ano_mes}   # secao 10
        total = round(sum(l["faturamento"] for l in linhas), 2)
        print(f"{ano_mes}: {len(linhas)} UFs carregadas, faturamento {total}")
        return {"ano_mes": ano_mes, "ufs": len(linhas), "faturamento": total}

    extrair >> transformar >> carregar()


pedidos_mensal()
EOF

```

Os pontos principais:

| Trecho | Função |
|---|---|
| `ANO_MES = "{{ data_interval_start... }}"` | Um **template Jinja**. O texto só vira `2024-03` na hora da execução, com os dados daquele DAG run. Templates funcionam nos campos *templated* dos operadores, como `bash_command` e `application_args` |
| `schedule=CronDataIntervalTimetable(...)` | A agenda: mensal, processando o mês anterior (a seção 7 explica) |
| `end_date` e `catchup=False` | Até quando agendar e se o Airflow deve "recuperar" execuções perdidas (seção 7) |
| `max_active_runs=2` | No máximo dois meses em processamento ao mesmo tempo |
| `default_args` | Parâmetros aplicados a todas as tasks: uma nova tentativa 30 s depois de uma falha |
| `pool="spark"` | O `transformar` ocupa o único slot do Pool `spark` |
| `outlets=[VENDAS_UF]` | Ao terminar com sucesso, `carregar` registra que o **Asset** `ecommerce.vendas_uf` foi atualizado (seção 10) |
| `extrair >> transformar >> carregar()` | As dependências: o operador `>>` encadeia tasks |

### 6.b. As tasks do DAG

```sh
# 6.b.1
aguardar-dag pedidos_mensal
airflow tasks list pedidos_mensal

```

Output esperado:
```
[aguardar-dag] DAG pedidos_mensal registrado.
[aguardar-dag] Nenhuma execucao de pedidos_mensal ate agora.
carregar
extrair
transformar
```

Na UI, abra o `pedidos_mensal` e alterne para a visão **Gráfico** (tecla `g`): as três tasks aparecem encadeadas, com o operador de cada uma:

![Visão Gráfico do DAG pedidos_mensal: extrair (BashOperator), transformar (SparkSubmitOperator) e carregar (@task) em sequência; à direita, a lista de execuções do DAG](images/airflow-05-pedidos-mensal-graph.png)

### 6.c. Vendo o template resolvido: `airflow tasks render`

Como fica o comando do `extrair` na execução de 1º de abril de 2024? Essa execução processa o mês que acabou de terminar, **março** (a seção 7 explica o porquê):

```sh
# 6.c.1
airflow tasks render pedidos_mensal extrair 2024-04-01 2>/dev/null | grep -A3 'bash_command'

```

Output esperado:
```
# property: bash_command
# ----------------------------------------------------------
mkdir -p /home/barbosa/project/datalake/raw/pedidos/ano_mes=2024-03 && cp /opt/lab-dados/pedidos/pedidos-2024-03.csv.gz /home/barbosa/project/datalake/raw/pedidos/ano_mes=2024-03/ && ls -l /home/barbosa/project/datalake/raw/pedidos/ano_mes=2024-03
# ----------------------------------------------------------
```

### 6.d. Testando o DAG inteiro

`airflow dags test` também aceita a data da execução. Simule a execução de 1º de janeiro de 2026, que processa **dezembro de 2025**, o mesmo mês do teste manual da seção 5.b:

```sh
# 6.d.1
airflow dags test pedidos_mensal 2026-01-01 2>&1 | grep -E 'UFs carregadas|DagRun Finished' | sed -E 's/(DagRun Finished: dag_id=[a-z_]+).*(state=[a-z]+).*/\1 ... \2/'

```

Output esperado:
```
2025-12: 20 UFs carregadas, faturamento 4790540.0
2026-11-27T12:25:09.438090Z [info     ] DagRun Finished: dag_id=pedidos_mensal ... state=success
```

O resultado no PostgreSQL:

```sh
# 6.d.2
psql -d ecommerce -c "SELECT * FROM vendas_uf WHERE ano_mes = '2025-12' ORDER BY faturamento DESC LIMIT 5;"

```

Output esperado:
```
 ano_mes | uf | pedidos | clientes | faturamento
---------+----+---------+----------+-------------
 2025-12 | RJ |     352 |      350 |   505200.00
 2025-12 | SP |     376 |      369 |   469900.00
 2025-12 | MG |     347 |      344 |   464750.00
 2025-12 | ES |     347 |      345 |   458160.00
 2025-12 | GO |     188 |      186 |   277850.00
(5 rows)
```

---

# Parte 3 — O tempo no Airflow

## 7. Logical date, data interval, catchup e backfill

### 7.a. Qual mês cada execução processa?

O DAG `pedidos_mensal` roda no dia 1º de cada mês. Mas no dia 1º de março, qual mês deve ser processado? **Fevereiro**, que acabou de terminar: os pedidos de março ainda nem existem. Cada DAG run de um DAG agendado cobre um **data interval** (intervalo de dados), e só pode rodar **depois** que o intervalo termina:

![Linha do tempo: o DAG run do intervalo de fevereiro (logical date 1º de fevereiro) só é disparado em 1º de março, quando o intervalo termina](diagramas/05-data-interval.svg)

| Conceito | No DAG run de fevereiro de 2024 |
|---|---|
| `data_interval_start` | 2024-02-01 00:00 |
| `data_interval_end` | 2024-03-01 00:00 |
| `logical_date` (data lógica, o "nome" do DAG run) | 2024-02-01 00:00 (= início do intervalo) |
| `run_after` (quando ele pode rodar) | 2024-03-01 00:00 (= fim do intervalo) |

Repare que o `run_id` de uma execução agendada ou de backfill usa a data em que ela roda: `backfill__2024-03-01T00:00:00+00:00` é a execução de **fevereiro**.

> **Que data eu passo para a CLI?** Depende do comando. No `airflow backfill create` e no `airflow tasks clear`, as datas são *logical dates*: `--from-date 2024-01-01` começa pelo intervalo de **janeiro**. No `airflow dags test`, no `airflow tasks render` e no `airflow dags trigger`, a data é o momento da execução (`run_after`), e o Airflow usa o intervalo que terminou nela: `2024-04-01` processa **março** (seção 6.c).

> **Atenção à mudança do Airflow 3.** Um `schedule` em texto (`"@monthly"` ou `"0 0 1 * *"`) usa por padrão o `CronTriggerTimetable`, que apenas **dispara** no horário marcado: o intervalo tem duração zero e o `data_interval_start` é o próprio momento do disparo. No Airflow 2 o padrão era o `CronDataIntervalTimetable`, com intervalos. Por isso o `pedidos_mensal` declara o `CronDataIntervalTimetable` explicitamente: com o padrão do Airflow 3, a execução do dia 1º de março processaria **março**.

Confira a agenda declarada:

```sh
# 7.a.1
airflow dags details pedidos_mensal -o plain 2>/dev/null | grep -E '^(timetable_summary|timetable_description|max_active_runs|next_dagrun_run_after) '

```

Output esperado:
```
timetable_summary                0 0 1 * *
timetable_description            At 00:00, on day 1 of the month
max_active_runs                  2
```

### 7.b. Despausando: `catchup=False`

O `start_date` é 1º de janeiro de 2024, mas hoje já estamos bem depois disso. O que acontece ao despausar?

```sh
# 7.b.1
airflow dags unpause pedidos_mensal >/dev/null
sleep 15
aguardar-dag pedidos_mensal

```

Output esperado:
```
[aguardar-dag] DAG pedidos_mensal registrado.
[aguardar-dag] Execucoes de pedidos_mensal:
  manual__2026-11-27T12:24:53.469603+00:00  success
```

Só existe a execução manual criada pelo `airflow dags test` da seção 6.d. O scheduler não criou nenhuma (e o `next_dagrun_run_after` da seção 7.a estava vazio):
- com `catchup=True`, ele criaria **um DAG run para cada intervalo** entre o `start_date` e hoje que ainda não tenha rodado: os 25 meses de uma vez;
- com `catchup=False` (o padrão no Airflow 3), ele agenda apenas o intervalo **mais recente**, e esse intervalo já passou do `end_date` (janeiro de 2026, o último arquivo). Não há nada a fazer automaticamente.

Para processar o histórico de forma controlada, o caminho é o **backfill**.

### 7.c. Backfill: processando o primeiro semestre de 2024

O backfill cria DAG runs para um período de *logical dates* (o início de cada intervalo). Processe de janeiro a junho de 2024 (6 execuções), no máximo duas ao mesmo tempo:

```sh
# 7.c.1
airflow backfill create --dag-id pedidos_mensal --from-date 2024-01-01 --to-date 2024-06-01 --max-active-runs 2 2>&1 | grep -c 'Created backfill Dag run'
aguardar-dag pedidos_mensal

```

Output esperado:
```
6
[aguardar-dag] DAG pedidos_mensal registrado.
[aguardar-dag] Execucoes de pedidos_mensal:
  backfill__2024-02-01T00:00:00+00:00  success
  backfill__2024-03-01T00:00:00+00:00  success
  backfill__2024-04-01T00:00:00+00:00  success
  backfill__2024-05-01T00:00:00+00:00  success
  backfill__2024-06-01T00:00:00+00:00  success
  backfill__2024-07-01T00:00:00+00:00  success
  manual__2026-11-27T12:24:53.469603+00:00  success
```

Enquanto ele roda, observe na UI a visão **Grade** do `pedidos_mensal`: duas execuções em andamento, mas só um `transformar` por vez. O outro fica em `scheduled`, esperando o slot do Pool `spark`. Ao final, cada coluna é um mês e cada linha, uma task (o ícone ↺ no pé da coluna indica uma execução de backfill):

![Visão Grade do DAG pedidos_mensal após o backfill: colunas de execuções com as três tasks em verde e, à direita, a Visão Geral com o gráfico de duração das execuções](images/airflow-06-pedidos-mensal-grid.png)

A ocupação dos Pools fica em **Administração > Pools** (ícone de engrenagem na barra lateral):

![Página de Pools: default_pool com 128 slots e spark com 1 slot e a descrição Jobs submetidos ao cluster Spark do laboratorio](images/airflow-08-pools.png)

Na UI do Spark Master (`http://<IP-PUBLICO>:9090`), cada mês processado aparece como uma aplicação concluída:

![UI do Spark Master: 1 worker ALIVE com 2 cores e a lista Completed Applications com as aplicações vendas_mensais de cada mês processado, todas FINISHED](images/spark-01-master.png)

 No PostgreSQL, os seis meses do backfill e dezembro de 2025, da seção 6.d:

```sh
# 7.c.2
psql -d ecommerce -c "SELECT ano_mes, count(*) AS ufs, sum(pedidos) AS pedidos, sum(faturamento) AS faturamento FROM vendas_uf GROUP BY ano_mes ORDER BY ano_mes;"

```

Output esperado:
```
 ano_mes | ufs | pedidos | faturamento
---------+-----+---------+-------------
 2024-01 |  20 |    1400 |  1898780.00
 2024-02 |  20 |    1600 |  2186670.00
 2024-03 |  20 |    2000 |  2702640.00
 2024-04 |  20 |    2000 |  2665070.00
 2024-05 |  20 |    2400 |  3190040.00
 2024-06 |  20 |    2000 |  2703000.00
 2025-12 |  20 |    3600 |  4790540.00
(7 rows)
```

E no datalake, uma partição por mês em cada camada:

```sh
# 7.c.3
ls ~/project/datalake/raw/pedidos ~/project/datalake/curated/vendas_uf

```

Output esperado:
```
/home/barbosa/project/datalake/curated/vendas_uf:
ano_mes=2024-01
ano_mes=2024-02
ano_mes=2024-03
ano_mes=2024-04
ano_mes=2024-05
ano_mes=2024-06
ano_mes=2025-12

/home/barbosa/project/datalake/raw/pedidos:
ano_mes=2024-01
ano_mes=2024-02
ano_mes=2024-03
ano_mes=2024-04
ano_mes=2024-05
ano_mes=2024-06
ano_mes=2025-12
```

### 7.d. Reprocessamento e idempotência: `airflow tasks clear`

Suponha que o arquivo de março de 2024 tenha sido corrigido na origem. Para reprocessar o mês, **limpe** (*clear*) as task instances daquele DAG run: o Airflow as devolve ao estado inicial e o scheduler as executa de novo. O `clear` seleciona as execuções pela *logical date*, o início do intervalo de março:

```sh
# 7.d.1
airflow tasks clear pedidos_mensal --start-date 2024-03-01 --end-date 2024-03-01 --yes 2>&1 | grep -E 'pedidos_mensal'
aguardar-dag pedidos_mensal

```

Output esperado:
```
[aguardar-dag] DAG pedidos_mensal registrado.
[aguardar-dag] Execucoes de pedidos_mensal:
  backfill__2024-02-01T00:00:00+00:00  success
  backfill__2024-03-01T00:00:00+00:00  success
  backfill__2024-04-01T00:00:00+00:00  success
  backfill__2024-05-01T00:00:00+00:00  success
  backfill__2024-06-01T00:00:00+00:00  success
  backfill__2024-07-01T00:00:00+00:00  success
  manual__2026-11-27T12:24:53.469603+00:00  success
```

Depois do reprocessamento, março continua com 20 UFs e o mesmo total: nada foi duplicado.

```sh
# 7.d.2
psql -d ecommerce -c "SELECT ano_mes, count(*) AS ufs, sum(pedidos) AS pedidos, sum(faturamento) AS faturamento FROM vendas_uf WHERE ano_mes = '2024-03' GROUP BY ano_mes;"

```

Output esperado:
```
 ano_mes | ufs | pedidos | faturamento
---------+-----+---------+-------------
 2024-03 |  20 |    2000 |  2702640.00
(1 row)
```

Na UI, a task `carregar` de março (execução `backfill__2024-04-01...`) mostra na aba **XCom** o dicionário devolvido pela função, e o campo **Número de tentativas** mostra que ela rodou duas vezes:

![Aba XCom da task carregar do DAG pedidos_mensal: return_value com ufs 20, ano_mes 2024-03 e faturamento 2702640; no cabeçalho, número de tentativas 2](images/airflow-07-carregar-xcom.png)

Isso só funciona porque **cada etapa é idempotente**: o `extrair` sobrescreve o arquivo na camada *raw*, o Spark sobrescreve só a partição do mês e o `carregar` apaga e reinsere o mês numa única transação. Se o `carregar` fizesse só `INSERT`, cada reprocessamento duplicaria o faturamento do mês. Num orquestrador, **toda task deve poder rodar de novo com segurança**: retries, backfills e clears fazem isso o tempo todo.

---

# Parte 4 — Resiliência

## 8. Retries, callbacks e trigger rules

### 8.a. Um DAG que falha de propósito

O DAG `instavel` simula quatro situações comuns:
- `consultar_api`: uma API que falha nas duas primeiras tentativas (falha transitória, resolvida com **retries**);
- `validar_arquivo`: uma falha definitiva (arquivo inexistente), sem retries e com um **callback** de falha que registra um alerta;
- `limpar`: uma limpeza que precisa rodar **mesmo que as anteriores falhem** (**trigger rule** `all_done`);
- `vigia`: uma task que só roda **se alguma anterior falhar** (`one_failed`) e falha de propósito, para que o DAG run termine como `failed` (a seção 8.b explica por que ela é necessária).

```sh
# 8.a.1
cat > ~/project/dags/instavel.py <<'EOF'
from datetime import datetime, timedelta

from airflow.sdk import dag, task

ALERTAS = "/home/barbosa/project/alertas.log"


def registrar_falha(context):
    """Callback: chamado pelo Airflow quando a task falha de vez (sem retries restantes)."""
    ti = context["ti"]
    with open(ALERTAS, "a") as arquivo:
        arquivo.write(f"[ALERTA] {ti.dag_id}.{ti.task_id} falhou na tentativa {ti.try_number}: "
                      f"{context['exception']}\n")


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=5)},
    tags=["lab"],
)
def instavel():

    @task
    def consultar_api(ti=None):
        if ti.try_number < 3:
            raise ConnectionError(f"Tentativa {ti.try_number}: API de frete indisponivel")
        print(f"Tentativa {ti.try_number}: API respondeu")

    @task(retries=0, on_failure_callback=registrar_falha)
    def validar_arquivo():
        raise FileNotFoundError("pedidos-2030-01.csv.gz nao encontrado na origem")

    @task(trigger_rule="all_done")
    def limpar():
        print("Limpeza executada (trigger_rule=all_done: roda mesmo com falhas antes)")

    @task(trigger_rule="one_failed", retries=0)
    def vigia():
        raise RuntimeError("Uma task anterior falhou: o DAG run precisa terminar como failed")

    etapas = [consultar_api(), validar_arquivo()]
    etapas >> limpar()
    etapas >> vigia()


instavel()
EOF

```

### 8.b. Executando

```sh
# 8.b.1
aguardar-dag instavel >/dev/null
airflow dags unpause instavel >/dev/null
airflow dags trigger instavel >/dev/null
aguardar-dag instavel

```

Output esperado:
```
[aguardar-dag] DAG instavel registrado.
[aguardar-dag] Execucoes de instavel:
  manual__2026-11-27T12:27:58.620479+00:00  failed
```

O DAG run terminou como `failed`. Por task:

```sh
# 8.b.2
RUN_ID=$(airflow dags list-runs instavel -o json | jq -r '.[0].run_id')
airflow tasks states-for-dag-run instavel "$RUN_ID" -o json | jq -r '.[] | "\(.task_id): \(if (.state // "") == "" then "sem estado" else .state end)"'

```

Output esperado:
```
validar_arquivo: failed
vigia: failed
consultar_api: success
limpar: success
```

Na visão **Gráfico** da execução, as tasks `validar_arquivo` e `vigia` aparecem em vermelho:

![Visão Gráfico da execução do DAG instavel: validar_arquivo e vigia em failed, consultar_api e limpar em success, e a execução marcada como Falha no painel da direita](images/airflow-09-instavel-graph.png)

Por que a `vigia` é necessária? O estado final de um DAG run é decidido pelas **tasks finais** (as folhas do grafo). Sem a `vigia`, a única folha seria a `limpar`, que roda com `all_done` e termina com sucesso, e o DAG run ficaria `success` mesmo com `validar_arquivo` em `failed`. Quem olhasse só a lista de execuções acharia que estava tudo bem. A `vigia` (`one_failed`) só roda quando algo falhou, e a falha dela leva o DAG run para `failed`. Esse padrão aparece na documentação do Airflow justamente para DAGs com tasks de limpeza `all_done`.

### 8.c. As tentativas de `consultar_api`

Cada tentativa tem o seu próprio arquivo de log. Entre uma e outra, a task ficou no estado `up_for_retry` durante o `retry_delay`:

```sh
# 8.c.1
ls $AIRFLOW_HOME/logs/dag_id=instavel/run_id=$RUN_ID/task_id=consultar_api/
cat $AIRFLOW_HOME/logs/dag_id=instavel/run_id=$RUN_ID/task_id=consultar_api/attempt=*.log | grep -oE 'Tentativa [0-9]: [^"]*' | sort -u

```

Output esperado:
```
attempt=1.log
attempt=2.log
attempt=3.log
Tentativa 1: API de frete indisponivel
Tentativa 2: API de frete indisponivel
Tentativa 3: API respondeu
```

### 8.d. O callback de falha

```sh
# 8.d.1
cat ~/project/alertas.log

```

Output esperado:
```
[ALERTA] instavel.validar_arquivo falhou na tentativa 1: pedidos-2030-01.csv.gz nao encontrado na origem
```

Em produção, o callback enviaria uma mensagem ao Slack, abriria um chamado ou dispararia um e-mail. Um callback no nível do DAG (`on_failure_callback` no `@dag`) também é comum para avisar que o pipeline inteiro falhou.

As *trigger rules* mais usadas:

| Trigger rule | A task roda quando... |
|---|---|
| `all_success` (padrão) | todas as tasks anteriores terminaram com sucesso |
| `all_done` | todas as anteriores terminaram, com qualquer resultado |
| `all_failed` | todas as anteriores falharam |
| `one_failed` | pelo menos uma anterior falhou (útil para alertas) |
| `none_failed` | nenhuma anterior falhou (as puladas, `skipped`, não contam como falha) |

![Ciclo de vida de uma task instance: scheduled, queued, running e os estados finais success e failed, com os desvios up_for_retry, deferred e skipped](diagramas/06-ciclo-de-vida-task.svg)

## 9. Sensores deferrable e o Triggerer

### 9.a. Esperando o arquivo chegar

Na prática, o arquivo do mês nem sempre está disponível na hora agendada. Um **sensor** é uma task que espera uma condição: um arquivo, uma partição, uma linha numa tabela, outro DAG. O DAG `pedidos_landing` espera um arquivo de pedidos chegar na pasta *landing* e então o processa:

```sh
# 9.a.1
cat > ~/project/dags/pedidos_landing.py <<'EOF'
import glob
import gzip
import os
import shutil
from datetime import datetime

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import dag, task

LANDING = "/home/barbosa/project/datalake/landing"


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False, tags=["ecommerce"])
def pedidos_landing():

    esperar_arquivo = FileSensor(
        task_id="esperar_arquivo",
        filepath=f"{LANDING}/pedidos-*.csv.gz",
        poke_interval=10,              # verifica a cada 10 s
        timeout=3600,                  # desiste depois de 1 h
        deferrable=True,               # espera no Triggerer, sem ocupar um slot de execucao
    )

    @task
    def processar():
        for arquivo in sorted(glob.glob(f"{LANDING}/pedidos-*.csv.gz")):
            with gzip.open(arquivo, "rt") as f:
                pedidos = sum(1 for _ in f) - 1          # menos o cabecalho
            destino = os.path.join(LANDING, "processados", os.path.basename(arquivo))
            os.makedirs(os.path.dirname(destino), exist_ok=True)
            shutil.move(arquivo, destino)
            print(f"{os.path.basename(arquivo)}: {pedidos} pedidos (movido para processados/)")

    esperar_arquivo >> processar()


pedidos_landing()
EOF
mkdir -p ~/project/datalake/landing

```

Dispare o DAG. Como a pasta *landing* está vazia, o sensor fica esperando:

```sh
# 9.a.2
aguardar-dag pedidos_landing >/dev/null
airflow dags unpause pedidos_landing >/dev/null
airflow dags trigger pedidos_landing >/dev/null
sleep 20
RUN_ID=$(airflow dags list-runs pedidos_landing -o json | jq -r '.[0].run_id')
airflow tasks states-for-dag-run pedidos_landing "$RUN_ID" -o json | jq -r '.[] | "\(.task_id): \(if (.state // "") == "" then "sem estado" else .state end)"'

```

Output esperado:
```
processar: sem estado
esperar_arquivo: deferred
```

### 9.b. O estado `deferred`

Um sensor comum (*poke*) ocupa um slot do executor o tempo todo, acordando a cada `poke_interval` só para verificar a condição. Com dezenas de sensores esperando horas, o executor fica cheio de tasks que não fazem nada. Com `deferrable=True`, a task **se suspende** (estado `deferred`) e entrega a espera ao **Triggerer**, que vigia milhares de condições num único processo assíncrono. Quando a condição é satisfeita, a task volta para a fila e continua.

O Triggerer registra quantas esperas está acompanhando:

```sh
# 9.b.1
grep 'triggers currently running' /var/log/lab/triggerer.log | tail -1

```

Output esperado:
```
2026-11-27T12:28:13.015502Z [info     ] 0 triggers currently running   [airflow.jobs.triggerer_job_runner] loc=triggerer_job_runner.py:1179
```

E se o Triggerer estiver fora do ar? Pare o serviço e coloque o arquivo de julho de 2024 na *landing*:

```sh
# 9.b.2
svc-stop triggerer
cp /opt/lab-dados/pedidos/pedidos-2024-07.csv.gz ~/project/datalake/landing/
sleep 30
airflow tasks states-for-dag-run pedidos_landing "$RUN_ID" -o json | jq -r '.[] | "\(.task_id): \(if (.state // "") == "" then "sem estado" else .state end)"'

```

Output esperado:
```
[triggerer] Parado (PID 521).
processar: sem estado
esperar_arquivo: deferred
```

O arquivo chegou, mas ninguém está vigiando: a task continua `deferred`. Religue o Triggerer:

```sh
# 9.b.3
svc-start triggerer
aguardar-dag pedidos_landing
airflow tasks states-for-dag-run pedidos_landing "$RUN_ID" -o json | jq -r '.[] | "\(.task_id): \(if (.state // "") == "" then "sem estado" else .state end)"'
jq -r 'select(.logger == "task.stdout") | .event' $AIRFLOW_HOME/logs/dag_id=pedidos_landing/run_id=$RUN_ID/task_id=processar/attempt=1.log

```

Output esperado:
```
[triggerer] Iniciado (PID 6750, log: /var/log/lab/triggerer.log).
[aguardar-dag] DAG pedidos_landing registrado.
[aguardar-dag] Execucoes de pedidos_landing:
  manual__2026-11-27T12:28:38.892837+00:00  success
esperar_arquivo: success
processar: success
pedidos-2024-07.csv.gz: 2000 pedidos (movido para processados/)
```

Quando o Triggerer voltou, ele retomou a espera, encontrou o arquivo e devolveu a task ao scheduler. A espera não se perdeu porque o estado da task estava no banco de metadados, e não na memória de um processo. Esse é o princípio de todo o Airflow.

---

# Parte 5 — Pipelines encadeados por dados

## 10. Assets, Dynamic Task Mapping e branching

### 10.a. Agendar por dados, não por horário

O relatório de vendas depende da tabela `vendas_uf`. Agendá-lo para "dia 1º às 3h" seria apostar que o `pedidos_mensal` terminou até lá. Com **Assets**, o relatório é agendado **pela atualização dos dados**: a task `carregar` declara `outlets=[VENDAS_UF]`; o DAG consumidor declara `schedule=[VENDAS_UF]` e roda sempre que o Asset é atualizado.

![Assets: a task carregar do pedidos_mensal atualiza o Asset ecommerce.vendas_uf, e cada atualização dispara um DAG run do relatorio_vendas](diagramas/07-assets.svg)

O relatório compara o faturamento do mês com uma meta, guardada numa **Variable** (configuração que pode mudar sem alterar o código do DAG):

```sh
# 10.a.1
airflow variables set meta_faturamento_mensal 3000000
airflow variables get meta_faturamento_mensal

```

Output esperado:
```
Variable meta_faturamento_mensal created
3000000
```

### 10.b. O DAG `relatorio_vendas`

```sh
# 10.b.1
cat > ~/project/dags/relatorio_vendas.py <<'EOF'
import os
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, Variable, dag, task

VENDAS_UF = Asset("ecommerce.vendas_uf")
RELATORIOS = "/home/barbosa/project/relatorios"


@dag(schedule=[VENDAS_UF], start_date=datetime(2024, 1, 1), catchup=False, tags=["ecommerce"])
def relatorio_vendas():

    @task
    def mes_carregado(triggering_asset_events=None):
        # o extra gravado pela task 'carregar' do pedidos_mensal diz qual mes foi atualizado
        eventos = triggering_asset_events[VENDAS_UF]
        ano_mes = eventos[-1].extra["ano_mes"]
        print(f"{len(eventos)} evento(s) do Asset; mes mais recente: {ano_mes}")
        return ano_mes

    @task
    def maiores_ufs(ano_mes: str):
        hook = PostgresHook(postgres_conn_id="postgres_ecommerce")
        ufs = hook.get_records(
            "SELECT uf FROM vendas_uf WHERE ano_mes = %s ORDER BY faturamento DESC LIMIT 3", parameters=(ano_mes,))
        return [{"ano_mes": ano_mes, "uf": uf} for (uf,) in ufs]

    @task
    def relatorio_uf(item: dict):
        hook = PostgresHook(postgres_conn_id="postgres_ecommerce")
        pedidos, clientes, faturamento = hook.get_first(
            "SELECT pedidos, clientes, faturamento FROM vendas_uf WHERE ano_mes = %s AND uf = %s",
            parameters=(item["ano_mes"], item["uf"]))
        os.makedirs(f"{RELATORIOS}/{item['ano_mes']}", exist_ok=True)
        with open(f"{RELATORIOS}/{item['ano_mes']}/{item['uf']}.txt", "w") as f:
            f.write(f"{item['uf']} em {item['ano_mes']}: {pedidos} pedidos, {clientes} clientes, R$ {faturamento}\n")
        print(f"Relatorio de {item['uf']} gravado")

    @task.branch
    def avaliar_meta(ano_mes: str):
        meta = float(Variable.get("meta_faturamento_mensal"))
        hook = PostgresHook(postgres_conn_id="postgres_ecommerce")
        (total,) = hook.get_first("SELECT sum(faturamento) FROM vendas_uf WHERE ano_mes = %s", parameters=(ano_mes,))
        print(f"{ano_mes}: faturamento {total} / meta {meta:.2f}")
        return "comemorar" if float(total) >= meta else "alertar"   # task_id do caminho escolhido

    @task
    def comemorar():
        print("Meta batida!")

    @task
    def alertar():
        print("Meta NAO atingida: avisar o time comercial")

    mes = mes_carregado()
    relatorio_uf.expand(item=maiores_ufs(mes))       # uma task instance por UF (Dynamic Task Mapping)
    avaliar_meta(mes) >> [comemorar(), alertar()]


relatorio_vendas()
EOF

```

Três recursos novos:

| Recurso | Onde | O que faz |
|---|---|---|
| **Asset como agenda** | `schedule=[VENDAS_UF]` e `triggering_asset_events` | O DAG roda a cada atualização do Asset e recebe os eventos, com o `extra` gravado pelo produtor (`{"ano_mes": "2024-07"}`) |
| **Dynamic Task Mapping** | `relatorio_uf.expand(item=...)` | O número de task instances de `relatorio_uf` só é conhecido na execução: uma para cada item da lista devolvida por `maiores_ufs` |
| **Branching** | `@task.branch` | A task devolve o `task_id` do caminho a seguir; as outras tasks a jusante ficam `skipped` |

### 10.c. Julho de 2024: o pipeline dispara o relatório

Despause o relatório e processe julho de 2024 no `pedidos_mensal`. Repare que você **não dispara** o `relatorio_vendas`:

```sh
# 10.c.1
aguardar-dag relatorio_vendas >/dev/null
airflow dags unpause relatorio_vendas >/dev/null
airflow backfill create --dag-id pedidos_mensal --from-date 2024-07-01 --to-date 2024-07-01 2>&1 | grep -c 'Created backfill Dag run'
aguardar-dag pedidos_mensal >/dev/null
sleep 10
aguardar-dag relatorio_vendas

```

Output esperado:
```
1
[aguardar-dag] DAG relatorio_vendas registrado.
[aguardar-dag] Execucoes de relatorio_vendas:
  asset_triggered__2026-11-27T12:30:21.840274+00:00_cjsU935S  success
```

O estado de cada task do relatório:

```sh
# 10.c.2
RUN_ID=$(airflow dags list-runs relatorio_vendas -o json | jq -r '.[0].run_id')
airflow tasks states-for-dag-run relatorio_vendas "$RUN_ID" -o json | jq -r '.[] | "\(.task_id)\(if .map_index != "" then "[\(.map_index)]" else "" end): \(if (.state // "") == "" then "sem estado" else .state end)"' | sort
cat ~/project/relatorios/2024-07/*.txt

```

Output esperado:
```
alertar: success
avaliar_meta: success
comemorar: skipped
maiores_ufs: success
mes_carregado: success
relatorio_uf[0]: success
relatorio_uf[1]: success
relatorio_uf[2]: success
ES em 2024-07: 199 pedidos, 199 clientes, R$ 271450.00
MG em 2024-07: 192 pedidos, 190 clientes, R$ 266910.00
SP em 2024-07: 212 pedidos, 212 clientes, R$ 275240.00
```

- `relatorio_uf` virou **três** task instances (`[0]`, `[1]`, `[2]`), uma para cada UF da lista. Na visão **Gráfico**, elas aparecem como um único nó `relatorio_uf [3]`; na lista de task instances, com o **Índice do Mapa** de cada uma.
- Em julho de 2024 o faturamento ficou abaixo da meta de R$ 3 milhões: o branch escolheu `alertar` e `comemorar` ficou `skipped`.

Na UI, a execução de julho na visão **Gráfico** do `relatorio_vendas`:

![Visão Gráfico da execução do relatorio_vendas disparada pelo Asset: mes_carregado, maiores_ufs, relatorio_uf [3], avaliar_meta e alertar em success, e comemorar em skipped (rosa)](images/airflow-10-relatorio-vendas-graph.png)

### 10.d. Novembro de 2024: a Black Friday

```sh
# 10.d.1
airflow backfill create --dag-id pedidos_mensal --from-date 2024-11-01 --to-date 2024-11-01 2>&1 | grep -c 'Created backfill Dag run'
aguardar-dag pedidos_mensal >/dev/null
sleep 10
aguardar-dag relatorio_vendas >/dev/null
RUN_ID=$(airflow dags list-runs relatorio_vendas -o json | jq -r 'sort_by(.run_after) | .[-1].run_id')
airflow tasks states-for-dag-run relatorio_vendas "$RUN_ID" -o json | jq -r '.[] | select(.task_id | test("meta|comemorar|alertar")) | "\(.task_id): \(if (.state // "") == "" then "sem estado" else .state end)"' | sort
jq -r 'select(.logger == "task.stdout") | .event' $AIRFLOW_HOME/logs/dag_id=relatorio_vendas/run_id=$RUN_ID/task_id=avaliar_meta/attempt=1.log

```

Output esperado:
```
1
alertar: skipped
avaliar_meta: success
comemorar: success
2024-11: faturamento 4920320.00 / meta 3000000.00
```

Com R$ 4,9 milhões em novembro, o branch seguiu para `comemorar`. O Asset e quem depende dele:

```sh
# 10.d.2
airflow assets list

```

Output esperado:
```
name                | uri                 | group | extra
====================+=====================+=======+======
ecommerce.vendas_uf | ecommerce.vendas_uf | asset | {}
```

Na UI, o menu **Assets** mostra o grafo produtor → Asset → consumidor e a lista de eventos de atualização, cada um com o `extra` gravado pelo `carregar` e o DAG run que ele disparou:

![Página do Asset ecommerce.vendas_uf: grafo pedidos_mensal → ecommerce.vendas_uf → relatorio_vendas e, à direita, os Eventos de Asset com ano_mes 2024-11 e 2024-07 e o link para a execução do relatorio_vendas que cada um disparou](images/airflow-11-asset.png)

## 11. Boas práticas

**O código de topo do arquivo roda o tempo todo.** O DAG Processor executa cada arquivo da pasta de DAGs a cada poucos segundos. Uma consulta a banco, uma chamada de API ou um `import` pesado fora das tasks deixa o parsing lento e sobrecarrega os sistemas consultados. Tudo o que é trabalho vai **dentro** das tasks. O tempo de parsing de cada arquivo:

```sh
# 11.1
airflow dags report

```

Output esperado:
```
file                | duration       | dag_num | task_num | dags
====================+================+=========+==========+=================
pedidos_mensal.py   | 0:00:00.133828 | 1       | 3        | pedidos_mensal
relatorio_vendas.py | 0:00:00.036208 | 1       | 6        | relatorio_vendas
pedidos_landing.py  | 0:00:00.002686 | 1       | 2        | pedidos_landing
instavel.py         | 0:00:00.002161 | 1       | 4        | instavel
ola_airflow.py      | 0:00:00.001546 | 1       | 2        | ola_airflow
```

**Tasks idempotentes e atômicas.** Rodar a mesma task duas vezes para o mesmo intervalo deve dar o mesmo resultado (seção 7.d). Grave por partição ou com `DELETE` + `INSERT` numa transação, nunca só com `INSERT`.

**O intervalo vem do Airflow.** Use `data_interval_start` / `data_interval_end` (ou `ds`, `logical_date`) em vez de `datetime.now()`. Com `now()`, um backfill de março de 2024 processaria o mês corrente.

**O Airflow orquestra; quem processa são outros sistemas.** Volume de dados vai para o Spark, para o banco, para o data warehouse. As tasks Python do Airflow devem fazer trabalho leve. O **XCom** é para metadados pequenos (um mês, uma contagem, um caminho), nunca para o conteúdo de um DataFrame.

**Configuração fora do código.** Endereços e credenciais em **Connections**; parâmetros de negócio em **Variables**; limites de recursos compartilhados em **Pools**.

**Teste antes de agendar.** `airflow dags list-import-errors` para erros de sintaxe, `airflow tasks render` para templates e `airflow dags test` para o fluxo completo.

---

# Apêndices

## A. Referência rápida de comandos

### Serviços do laboratório

| Comando | Descrição |
|---|---|
| `svc-status` | Estado dos serviços (postgres, spark-master, spark-worker, api-server, scheduler, dag-processor, triggerer) |
| `svc-stop <serviço>` / `svc-start <serviço>` / `svc-restart <serviço>` | Para, inicia ou reinicia um serviço (`all` para todos) |
| `aguardar-dag <dag_id>` | Espera o DAG ser registrado e as execuções terminarem; mostra o estado de cada uma |
| `cat /var/log/lab/<serviço>.log` | Log de um serviço |

### Airflow CLI

| Comando | Descrição |
|---|---|
| `airflow dags list` / `airflow dags list-import-errors` | DAGs registrados / erros de importação |
| `airflow dags unpause <dag>` / `airflow dags pause <dag>` | Liga / desliga o agendamento |
| `airflow dags trigger <dag>` | Dispara uma execução manual |
| `airflow dags test <dag> [data]` | Executa o DAG inteiro num único processo, sem scheduler |
| `airflow dags list-runs <dag>` | Execuções do DAG |
| `airflow tasks list <dag>` | Tasks do DAG |
| `airflow tasks states-for-dag-run <dag> <run_id>` | Estado de cada task de uma execução |
| `airflow tasks render <dag> <task> <data>` | Mostra os templates resolvidos |
| `airflow tasks test <dag> <task> [data]` | Executa uma única task, sem registrar estado |
| `airflow tasks clear <dag> -s <início> -e <fim>` | Limpa task instances para que rodem de novo |
| `airflow backfill create --dag-id <dag> --from-date <d> --to-date <d>` | Cria execuções para um período |
| `airflow connections add/list/delete` | Connections |
| `airflow variables set/get/list` | Variables |
| `airflow pools set/list` | Pools |
| `airflow assets list` | Assets |
| `airflow config get-value <seção> <chave>` | Valor efetivo de uma configuração |

## B. Encerrando o ambiente

Ao fim de cada aula, pare a instância: no Console AWS (`us-east-1`), **EC2** > **Instances**, selecione a sua instância **`lab-airflow-orchestration-<data e hora>`** e clique em **Instance state** > **Stop instance**. Quando o laboratório tiver terminado de vez, use **Instance state** > **Terminate instance**.

## C. Troubleshooting

Rode o diagnóstico completo do ambiente:

```sh
curl -sS https://raw.githubusercontent.com/infobarbosa/airflow-orchestration-demo/main/scripts/check.sh | bash

```

| Sintoma | O que verificar |
|---|---|
| O DAG não aparece na lista | `airflow dags list-import-errors` mostra erros de Python no arquivo; o DAG Processor leva até ~10 s para ver um arquivo novo |
| O DAG aparece, mas nada roda | O DAG está pausado? (`airflow dags unpause <dag>`); o scheduler está `UP` em `svc-status`? |
| Task parada em `scheduled` | Falta slot no Pool (`airflow pools list`) ou o DAG atingiu `max_active_runs` |
| Task parada em `deferred` | O triggerer está `UP`? (seção 9.b) |
| `The conn_id ... isn't defined` | Crie as Connections da seção 4.b |
| A UI do Airflow não abre | `svc-status` e `tail -50 /var/log/lab/api-server.log` |

Para reiniciar todos os serviços (o banco e os seus arquivos são preservados):

```sh
svc-restart all

```

## D. Execução local

Para executar localmente no Docker (cerca de 4 GB de RAM livres), em `amd64` ou `arm64` (Mac com Apple Silicon):

```sh
mkdir airflow-orchestration-demo && cd airflow-orchestration-demo
curl -sSO https://raw.githubusercontent.com/infobarbosa/airflow-orchestration-demo/main/compose.yaml
docker compose up -d

```

Ou diretamente:

```sh
docker run -d --name airflow-lab -p 8080:8080 -p 8081:8081 -p 9090:9090 -p 8082:8082 --stop-timeout 60 ghcr.io/infobarbosa/airflow-lab-docker-image:latest

```

Acesse a IDE em `http://localhost:8080`, o Airflow em `http://localhost:8081` e o Spark Master em `http://localhost:9090`.

---

## Parabéns!

Você construiu um pipeline de dados orquestrado de ponta a ponta: agendado por intervalos de dados, testado antes de rodar, reprocessável sem duplicar dados, resiliente a falhas e encadeado a um segundo pipeline pela atualização dos dados. No laboratório [airflow-cluster-administration](https://github.com/infobarbosa/airflow-cluster-administration), o foco passa a ser o outro lado: operar o Airflow em cluster, com CeleryExecutor, vários schedulers e workers, e o que acontece quando cada componente cai.
