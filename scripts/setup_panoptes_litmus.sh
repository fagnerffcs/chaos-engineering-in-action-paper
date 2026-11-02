#!/usr/bin/env bash
# ==============================================================================
# Prepares the PANOPTES + LitmusChaos K3d Environment for the Paired Benchmark
# ==============================================================================
set -euo pipefail

echo "======================================================================"
echo "  PREPARING PANOPTES + LITMUSCHAOS ENVIRONMENT (K3D STRICT PARITY)"
echo "======================================================================"

# 1. Stop the Vanilla cluster if running to preserve host RAM parity
echo "[1/6] Stopping Vanilla cluster (panoptes-cluster) if active..."
k3d cluster stop panoptes-cluster 2>/dev/null || true

# 2. Start or create panoptes-cluster-scenario2 (verifying server-0 container actually exists)
if docker ps -a --format '{{.Names}}' | grep -q "^k3d-panoptes-cluster-scenario2-server-0$"; then
  echo "[2/6] Starting existing 'panoptes-cluster-scenario2'..."
  k3d cluster start panoptes-cluster-scenario2
  for node in panoptes-cluster-scenario2-server-0 panoptes-cluster-scenario2-agent-0 panoptes-cluster-scenario2-agent-1; do
    docker update --cpus="2.0" --memory="4g" --memory-swap="4g" "k3d-$node" 2>/dev/null || true
  done
  k3d kubeconfig write panoptes-cluster-scenario2 --output ~/.kube/config
  chmod 600 ~/.kube/config
else
  echo "[2/6] Server container missing in 'panoptes-cluster-scenario2'. Recreating cleanly..."
  k3d cluster delete panoptes-cluster-scenario2 2>/dev/null || true
  k3d cluster create panoptes-cluster-scenario2 \
    --servers 1 \
    --agents 2 \
    --k3s-arg "--disable=traefik@server:0" \
    --wait
  for node in panoptes-cluster-scenario2-server-0 panoptes-cluster-scenario2-agent-0 panoptes-cluster-scenario2-agent-1; do
    docker update --cpus="2.0" --memory="4g" --memory-swap="4g" "k3d-$node" 2>/dev/null || true
  done
  kubectl --context k3d-panoptes-cluster-scenario2 taint nodes k3d-panoptes-cluster-scenario2-server-0 node-role.kubernetes.io/master=true:NoSchedule --overwrite || true
  mkdir -p ~/.kube
  k3d kubeconfig write panoptes-cluster-scenario2 --output ~/.kube/config
  chmod 600 ~/.kube/config
fi

# 3. Ensure PANOPTES stack (monitoring + otel-demo + chaos-mesh) is deployed
if ! kubectl get ns monitoring >/dev/null 2>&1; then
  echo "[3/6] Deploying PANOPTES stack via Ansible (deploy-panoptes.yml)..."
  ansible-playbook -i inventory.ini deploy-panoptes.yml
else
  echo "[3/6] Namespace 'monitoring' already present on panoptes-cluster-scenario2."
fi

# 4. Enforce ADR-0005 workload scoping (scale down all 8 non-transactional bloat pods)
echo "[4/6] Enforcing ADR-0005 workload scoping in 'otel-demo'..."
kubectl scale statefulset opensearch -n otel-demo --replicas=0 2>/dev/null || true
kubectl scale deployment accounting fraud-detection agent chatbot mcp opamp-server telemetry-docs -n otel-demo --replicas=0 2>/dev/null || true

# 5. Install LitmusChaos Operator & CRDs (litmus-core + kubernetes-chaos) via Helm
echo "[5/6] Installing LitmusChaos Operator (litmus-core) and Kubernetes Chaos Experiments..."
helm repo add litmuschaos https://litmuschaos.github.io/litmus-helm/ >/dev/null 2>&1 || true
helm repo update >/dev/null 2>&1 || true

kubectl create ns litmus 2>/dev/null || true
helm upgrade --install litmus-core litmuschaos/litmus-core -n litmus --wait --timeout 5m
helm upgrade --install k8s-chaos litmuschaos/kubernetes-chaos -n otel-demo --wait --timeout 5m

echo "Waiting for Litmus chaos-operator to become Ready..."
kubectl wait --for=condition=Ready pods --all -n litmus --timeout=180s || true

# 6. Pre-pull Litmus runner images onto K3d nodes to eliminate network download bias in RQ01
echo "[6/6] Pre-pulling LitmusChaos runner images into K3d cluster nodes (Internal Validity)..."
RUNNER_IMG=$(kubectl get deploy -n litmus -o jsonpath='{.items[0].spec.template.spec.containers[0]. env[?(@.name=="CHAOS_RUNNER_IMAGE")].value}' 2>/dev/null || echo "litmuschaos/chaos-runner:3.31.0")
GO_RUNNER_IMG=$(kubectl get chaosexperiment pod-network-latency -n otel-demo -o jsonpath='{.spec.definition.image}' 2>/dev/null || echo "litmuschaos/go-runner:3.31.0")
for img in "$RUNNER_IMG" "$GO_RUNNER_IMG"; do
  if [ -n "$img" ]; then
    docker pull "$img" && k3d image import "$img" -c panoptes-cluster-scenario2 || true
  fi
done

echo ""
echo "======================================================================"
echo "  [OK] PANOPTES + LITMUSCHAOS ENVIRONMENT READY!"
echo "  Run './check-panoptes-health.sh' and then './run-litmus-paired-trials.py'"
echo "======================================================================"
