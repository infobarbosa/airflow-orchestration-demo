#!/bin/bash
#
# Verificacao do ambiente do laboratorio airflow-orchestration-demo.
# Uso (no terminal do code-server):
#   curl -sS https://raw.githubusercontent.com/infobarbosa/airflow-orchestration-demo/main/scripts/check.sh | bash
#

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

FALHAS=0
ok()    { echo -e "  ${GREEN}✓${NC} $1"; }
falha() { echo -e "  ${RED}✗${NC} $1"; FALHAS=$((FALHAS + 1)); }
aviso() { echo -e "  ${YELLOW}!${NC} $1"; }
secao() { echo -e "\n${BOLD}$1${NC}"; }

secao "Memória da Máquina"
MEM_MB=$(awk '/MemTotal/ {printf "%d", $2/1024}' /proc/meminfo)
if [ "${MEM_MB}" -ge 3500 ]; then
    ok "Memória RAM detectada: ${MEM_MB} MB"
else
    falha "Memória RAM detectada: ${MEM_MB} MB (o laboratório precisa de pelo menos 4 GB)"
fi

secao "Ferramentas"
for t in airflow spark-submit psql jq tree svc-status aguardar-dag; do
    if command -v "$t" >/dev/null 2>&1; then ok "$t"; else falha "$t não encontrado"; fi
done

secao "Serviços"
for s in postgres spark-master spark-worker api-server scheduler dag-processor triggerer; do
    estado=$(svc-status 2>/dev/null | awk -v s="$s" '$1 == s {print $2}')
    if [ "${estado}" = "UP" ]; then ok "${s}: UP"; else falha "${s}: ${estado:-desconhecido} (veja /var/log/lab/${s}.log)"; fi
done

secao "Airflow"
VERSAO=$(airflow version 2>/dev/null | tail -1)
[ -n "${VERSAO}" ] && ok "Airflow versão ${VERSAO}" || falha "comando 'airflow version' falhou"
EXECUTOR=$(airflow config get-value core executor 2>/dev/null)
[ "${EXECUTOR}" = "LocalExecutor" ] && ok "Executor: ${EXECUTOR}" || falha "Executor inesperado: ${EXECUTOR:-?}"
HEALTH=$(curl -s --max-time 5 http://localhost:8081/api/v2/monitor/health)
for c in metadatabase scheduler dag_processor triggerer; do
    st=$(echo "${HEALTH}" | jq -r ".${c}.status // empty" 2>/dev/null)
    if [ "${st}" = "healthy" ]; then ok "${c}: healthy"; else falha "${c}: ${st:-sem resposta} (API em http://localhost:8081)"; fi
done

secao "Spark"
SPARK_JSON=$(curl -s --max-time 5 http://localhost:9090/json/)
WORKERS=$(echo "${SPARK_JSON}" | jq '[.workers[]? | select(.state == "ALIVE")] | length' 2>/dev/null)
if [ "${WORKERS:-0}" -ge 1 ]; then
    ok "Spark Master com ${WORKERS} worker(s) ativo(s), $(echo "${SPARK_JSON}" | jq -r '.cores') cores"
else
    falha "Spark Master sem workers ativos (http://localhost:9090)"
fi
SPARK_VERSAO=$(spark-submit --version 2>&1 | grep -oE 'version [0-9]+\.[0-9]+\.[0-9]+' | head -1)
[ -n "${SPARK_VERSAO}" ] && ok "Spark ${SPARK_VERSAO}" || aviso "não foi possível ler a versão do Spark"

secao "Dados de origem"
ARQUIVOS=$(ls /opt/lab-dados/pedidos/pedidos-*.csv.gz 2>/dev/null | wc -l)
[ "${ARQUIVOS}" -eq 25 ] && ok "25 arquivos mensais de pedidos (2024-01 a 2026-01)" || falha "${ARQUIVOS} arquivos de pedidos em /opt/lab-dados/pedidos (esperado: 25)"
psql -d ecommerce -tAc "SELECT 1" >/dev/null 2>&1 && ok "banco ecommerce acessível" || falha "banco ecommerce inacessível"

secao "Resultado"
if [ "${FALHAS}" -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}Ambiente pronto para o laboratório.${NC}"
else
    echo -e "  ${RED}${BOLD}${FALHAS} verificação(ões) falharam.${NC} Aguarde 1 minuto e rode de novo; se persistir, veja o Apêndice C do README."
    exit 1
fi
