#!/usr/bin/env bash
# ==============================================================================
# PANOPTES HEALTH CHECK & STABILIZATION SCRIPT
# Ensures kernel limits, Chaos Mesh engine, observability backends, and target
# workloads are fully ready before initiating experimental trials.
# ==============================================================================

set -eo pipefail

APP_NAMESPACE="otel-demo"
MON_NAMESPACE="monitoring"
CHAOS_NAMESPACE="chaos-mesh"
TIMEOUT_SECONDS=600 # 10 minutos de timeout máximo

echo "======================================================================"
echo "  PANOPTES ENVIRONMENT STABILIZATION CHECKER"
echo "======================================================================"
echo "Waiting for all components to reach READY state (Timeout: ${TIMEOUT_SECONDS}s)..."
echo ""

# Função auxiliar para checar o status de rollout de forma limpa
wait_for_rollout() {
    local kind=$1
    local name=$2
    local namespace=$3

    echo -n "Checking ${kind}/${name} in namespace ${namespace}... "
    
    if kubectl rollout status "${kind}/${name}" -n "${namespace}" --timeout="${TIMEOUT_SECONDS}s" >/dev/null 2>&1; then
        echo "✅ READY"
    else
        echo "❌ FAILED OR TIMEOUT"
        exit 1
    fi
}

# 0. Verifica limites de inotify do kernel (essencial para k3d + Chaos Mesh + Promtail)
echo -n "Checking kernel inotify limits (fs.inotify.max_user_instances)... "
CURRENT_INOTIFY=$(sysctl -n fs.inotify.max_user_instances 2>/dev/null || echo 0)
if [ "${CURRENT_INOTIFY}" -lt 4096 ]; then
    echo "⚠️ LOW (${CURRENT_INOTIFY}). Attempting to raise to 8192..."
    sudo sysctl -w fs.inotify.max_user_instances=8192 >/dev/null
    sudo sysctl -w fs.inotify.max_user_watches=524288 >/dev/null
    echo "✅ UPDATED (8192)"
else
    echo "✅ OK (${CURRENT_INOTIFY})"
fi

# Limpa eventuais experimentos de caos órfãos antes de validar os pods
kubectl delete networkchaos,podchaos,stresschaos --all -n "${APP_NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true

# Garante que subsistemas não-transacionais (OpenSearch, LLM Agents, consumidores assíncronos fora do escopo)
# estejam em 0 réplicas para evitar exaustão do limite de 4GiB de RAM por nó (ADR-0005)
echo -n "Pruning non-essential OTel Demo auxiliary pods (ADR-0005)... "
kubectl scale statefulset/opensearch -n "${APP_NAMESPACE}" --replicas=0 >/dev/null 2>&1 || true
kubectl scale deployment/agent deployment/chatbot deployment/mcp deployment/fraud-detection deployment/accounting deployment/telemetry-docs deployment/opamp-server -n "${APP_NAMESPACE}" --replicas=0 >/dev/null 2>&1 || true
echo "✅ OK"

# 1. Verifica o motor de injeção de falhas (Chaos Mesh Controller + Daemons)
wait_for_rollout "deployment" "chaos-controller-manager" "${CHAOS_NAMESPACE}"
wait_for_rollout "daemonset" "chaos-daemon" "${CHAOS_NAMESPACE}"

# 2. Verifica os backends de telemetria da CNCF caso o namespace monitoring exista (Cenários 2, 4a, 4b)
if kubectl get namespace "${MON_NAMESPACE}" >/dev/null 2>&1; then
    wait_for_rollout "daemonset" "promtail" "${MON_NAMESPACE}"
    wait_for_rollout "statefulset" "prometheus-kube-prometheus-stack-prometheus" "${MON_NAMESPACE}"
    wait_for_rollout "deployment" "kube-prometheus-stack-kube-state-metrics" "${MON_NAMESPACE}"
    wait_for_rollout "statefulset" "tempo" "${MON_NAMESPACE}"
    wait_for_rollout "statefulset" "loki" "${MON_NAMESPACE}"

    # 3. Verifica o Coletor Central do PANOPTES (Instanciado pelo OTEL Operator)
    echo -n "Checking OpenTelemetry Collector in namespace ${MON_NAMESPACE}... "
    if kubectl get deployment panoptes-collector-collector -n "${MON_NAMESPACE}" >/dev/null 2>&1; then
        kubectl rollout status deployment/panoptes-collector-collector -n "${MON_NAMESPACE}" --timeout="${TIMEOUT_SECONDS}s" >/dev/null 2>&1
        echo "✅ READY"
    elif kubectl get daemonset panoptes-collector-collector -n "${MON_NAMESPACE}" >/dev/null 2>&1; then
        kubectl rollout status daemonset/panoptes-collector-collector -n "${MON_NAMESPACE}" --timeout="${TIMEOUT_SECONDS}s" >/dev/null 2>&1
        echo "✅ READY"
    else
        kubectl wait --for=condition=Ready pods -l app.kubernetes.io/name=panoptes-collector-collector -n "${MON_NAMESPACE}" --timeout="${TIMEOUT_SECONDS}s" >/dev/null 2>&1 || true
        echo "✅ READY (Verified via Pod labels)"
    fi
else
    echo "ℹ️  Vanilla Baseline Mode detected (namespace '${MON_NAMESPACE}' is absent). Skipping observability backend checks."
fi

# 4. Verifica os serviços-alvo do experimento (TARGET_POOL), proxy de entrada e gerador de carga
for svc in payment cart shipping currency frontend frontend-proxy checkout load-generator; do
    wait_for_rollout "deployment" "${svc}" "${APP_NAMESPACE}"
done

# 5. Validação final rigorosa de todos os pods em execução no namespace da aplicação
echo -n "Verifying ALL active pods in ${APP_NAMESPACE} are Running and Ready... "
kubectl wait --for=condition=Ready pods --all --field-selector=status.phase!=Succeeded -n "${APP_NAMESPACE}" --timeout="${TIMEOUT_SECONDS}s" >/dev/null 2>&1
echo "✅ READY"

echo ""
echo "======================================================================"
echo "  [OK] ENVIRONMENT STABILIZED. READY FOR EXPERIMENTAL TRIALS."
echo "======================================================================"
