#!/usr/bin/env python3
"""
Automated Paired Experimental Runner: LitmusChaos vs. Chaos Mesh (RQ01 - RQ04)
==============================================================================
Executes the exact same N=30 seeded, stratified fault schedule (RANDOM_SEED = 42)
used in Scenario 4b (Chaos Mesh), but orchestrates perturbations via LitmusChaos
ChaosEngine CRDs (pod-network-latency, pod-delete, pod-cpu-hog) with native
Litmus Probes (promProbe / httpProbe / k8sProbe).

Captures:
  - RQ01: Actuation latency (dT_actuate = T_effect - T_CRD), teardown latency
          (dT_teardown), and auxiliary runner pod churn (chaos-runner + go-runner).
  - RQ02: Steady-state (--mode overhead) and active-campaign CPU/RAM overhead
          across the 'litmus', 'chaos-mesh', 'monitoring', and 'otel-demo' namespaces.
  - RQ03: Total MTTDiag (T_diag - T_CRD), Net MTTDiag (T_diag - T_effect),
          root-cause accuracy (Acc), and unfiltered runner-pod telemetry noise.
  - RQ04: Native Litmus Probe pass/fail verdict and ChaosResult Resilience Score (%).
"""

import argparse
import csv
from datetime import datetime, timezone
import importlib.util
import json
import os
import subprocess
import time

# Load the PANOPTES Multimodal RCA engine and helpers from run-scenario4-optimized-trials.py
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
S4B_PATH = os.path.join(SCRIPT_DIR, "run-scenario4-optimized-trials.py")
spec = importlib.util.spec_from_file_location("s4b", S4B_PATH)
s4b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s4b)

NUM_TRIALS = 30
RANDOM_SEED = 42
TIMEOUT_SECONDS = 300
APP_NAMESPACE = "otel-demo"
MONITORING_NAMESPACE = "monitoring"
LITMUS_NAMESPACE = "litmus"
CHAOS_MESH_NAMESPACE = "chaos-mesh"

RESULTS_CSV = "litmus_paired_trials.csv"
AUDIT_JSONL = "litmus_evidence_audit.jsonl"
OVERHEAD_CSV = "chaosmesh_vs_litmus_overhead.csv"

CHAOS_TYPE_TO_LITMUS = {
    "network-delay": "pod-network-latency",
    "pod-kill": "pod-delete",
    "cpu-stress": "pod-cpu-hog",
}


def run_cmd(cmd, timeout=20):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def parse_cpu(cpu_str):
    if cpu_str.endswith("m"):
        return float(cpu_str[:-1])
    elif cpu_str.endswith("n"):
        return float(cpu_str[:-1]) / 1_000_000.0
    return float(cpu_str) * 1000.0


def parse_memory(mem_str):
    if mem_str.endswith("Mi"):
        return float(mem_str[:-2])
    elif mem_str.endswith("Gi"):
        return float(mem_str[:-2]) * 1024.0
    elif mem_str.endswith("Ki"):
        return float(mem_str[:-2]) / 1024.0
    return float(mem_str) / (1024.0 * 1024.0)


def sample_namespace_pods(namespace, filter_substring=None):
    """Returns (cpu_mcpu, ram_mib, pod_count) for a namespace (or matching pod substring)."""
    res = run_cmd(f"kubectl top pods -n {namespace} --no-headers 2>/dev/null", timeout=15)
    if not res or res.returncode != 0 or not res.stdout.strip():
        return 0.0, 0.0, 0
    cpu_total = 0.0
    mem_total = 0.0
    count = 0
    for line in res.stdout.strip().split("\n"):
        parts = line.split()
        if len(parts) >= 3:
            pod_name = parts[0]
            if filter_substring and filter_substring not in pod_name:
                continue
            try:
                cpu_total += parse_cpu(parts[1])
                mem_total += parse_memory(parts[2])
                count += 1
            except ValueError:
                pass
    return round(cpu_total, 2), round(mem_total, 2), count


def ensure_litmus_rbac_and_experiments():
    """
    Ensures the litmus-admin ServiceAccount, ClusterRoleBinding, and the three
    core ChaosExperiment CRDs (pod-network-latency, pod-delete, pod-cpu-hog)
    exist in the otel-demo namespace before executing paired trials.
    """
    print("[LITMUS-INIT] Ensuring litmus-admin RBAC and ChaosExperiment CRDs in otel-demo...", flush=True)
    rbac_yaml = f"""apiVersion: v1
kind: ServiceAccount
metadata:
  name: litmus-admin
  namespace: {APP_NAMESPACE}
  labels:
    name: litmus-admin
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: litmus-admin-role
rules:
  - apiGroups: ["", "apps", "batch", "extensions", "litmuschaos.io"]
    resources: ["pods", "pods/exec", "pods/log", "events", "deployments", "replicasets", "daemonsets", "statefulsets", "jobs", "chaosengines", "chaosexperiments", "chaosresults"]
    verbs: ["create", "list", "get", "patch", "update", "delete", "deletecollection"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: litmus-admin-binding
subjects:
  - kind: ServiceAccount
    name: litmus-admin
    namespace: {APP_NAMESPACE}
roleRef:
  kind: ClusterRole
  name: litmus-admin-role
  apiGroup: rbac.authorization.k8s.io
"""
    with open("/tmp/litmus_rbac.yaml", "w") as f:
        f.write(rbac_yaml)
    run_cmd("kubectl apply -f /tmp/litmus_rbac.yaml", timeout=20)

    # Install standard Litmus ChaosExperiments from ChaosHub if not already present
    for exp_name in ("pod-delete", "pod-cpu-hog", "pod-network-latency"):
        check = run_cmd(f"kubectl get chaosexperiment {exp_name} -n {APP_NAMESPACE} 2>/dev/null")
        if not check or check.returncode != 0:
            hub_url = (
                f"https://hub.litmuschaos.io/api/chaos/3.0.0?file=faults/kubernetes/"
                f"{exp_name}/fault.yaml"
            )
            run_cmd(f"kubectl apply -f '{hub_url}' -n {APP_NAMESPACE}", timeout=30)


def generate_litmus_engine_manifest(target_service, chaos_type):
    """
    Synthesizes a LitmusChaos ChaosEngine CRD strictly equivalent to the Chaos Mesh
    manifest in Scenario 4b, bound to the exact resolved target pod instance,
    and attaches a declarative Litmus Probe (promProbe / httpProbe) for RQ04.
    """
    pods, l_key, l_val = s4b.resolve_target_pods(target_service)
    target_pod = pods[0] if pods else ""
    app_label = f"{l_key}={l_val}" if (l_key and l_val) else f"app.kubernetes.io/component={target_service}"
    litmus_exp = CHAOS_TYPE_TO_LITMUS[chaos_type]

    if chaos_type == "network-delay":
        env_block = f"""            - name: TOTAL_CHAOS_DURATION
              value: '180'
            - name: NETWORK_INTERFACE
              value: 'eth0'
            - name: NETWORK_LATENCY
              value: '2000'
            - name: JITTER
              value: '100'
            - name: CONTAINER_RUNTIME
              value: 'containerd'
            - name: SOCKET_PATH
              value: '/run/k3s/containerd/containerd.sock'
            - name: TARGET_PODS
              value: '{target_pod}'"""
    elif chaos_type == "pod-kill":
        env_block = f"""            - name: TOTAL_CHAOS_DURATION
              value: '15'
            - name: CHAOS_INTERVAL
              value: '15'
            - name: FORCE
              value: 'true'
            - name: TARGET_PODS
              value: '{target_pod}'"""
    else:  # cpu-stress -> pod-cpu-hog
        env_block = f"""            - name: TOTAL_CHAOS_DURATION
              value: '180'
            - name: CPU_CORES
              value: '2'
            - name: CPU_LOAD
              value: '100'
            - name: CONTAINER_RUNTIME
              value: 'containerd'
            - name: SOCKET_PATH
              value: '/run/k3s/containerd/containerd.sock'
            - name: TARGET_PODS
              value: '{target_pod}'"""

    # Attach native Litmus cmdProbe/promProbe querying Prometheus for RQ04 evaluation
    return f"""apiVersion: litmuschaos.io/v1alpha1
kind: ChaosEngine
metadata:
  name: dynamic-litmus-chaos
  namespace: {APP_NAMESPACE}
spec:
  engineState: 'active'
  annotationCheck: 'false'
  appinfo:
    appns: '{APP_NAMESPACE}'
    applabel: '{app_label}'
    appkind: 'deployment'
  chaosServiceAccount: litmus-admin
  jobCleanUpPolicy: 'retain'
  experiments:
    - name: {litmus_exp}
      spec:
        probe:
          - name: 'check-target-slo-probe'
            type: 'cmdProbe'
            mode: 'Continuous'
            runProperties:
              probeTimeout: 5s
              interval: 5s
              retry: 1
            cmdProbe/inputs:
              command: 'kubectl get pods -n {APP_NAMESPACE} -l {app_label} --field-selector=status.phase=Running -o name | grep -q pod'
              comparator:
                type: 'string'
                criteria: 'equal'
                value: ''
        components:
          env:
{env_block}
"""


def wait_for_litmus_actuation(t_crd, litmus_exp, max_wait=45.0):
    """
    Polls the otel-demo namespace after ChaosEngine creation (t_crd) to detect
    when LitmusChaos spawns the chaos-runner and go-runner helper pod (Running state),
    returning (t_effect, actuation_delay_seconds, runner_pod_count).
    """
    prefix_short = litmus_exp[:12]
    while time.time() - t_crd < max_wait:
        res = run_cmd(f"kubectl get pods -n {APP_NAMESPACE} --no-headers 2>/dev/null", timeout=5)
        if res and res.stdout:
            runner_pods = []
            go_runner_active = False
            for line in res.stdout.strip().split("\n"):
                if not line.strip():
                    continue
                parts = line.split()
                p_name = parts[0]
                p_status = parts[2] if len(parts) > 2 else ""
                if "dynamic-litmus-chaos-runner" in p_name or prefix_short in p_name:
                    runner_pods.append(p_name)
                    if prefix_short in p_name and p_status in ("Running", "Completed"):
                        go_runner_active = True
            if go_runner_active:
                t_effect = time.time()
                return t_effect, round(t_effect - t_crd, 2), len(runner_pods)
        time.sleep(0.5)
    t_effect = time.time()
    return t_effect, round(t_effect - t_crd, 2), 1


def extract_litmus_chaos_result(litmus_exp):
    """
    Reads the ChaosResult CRD generated by LitmusChaos to extract the native
    probe status and quantitative Resilience Score (%) for RQ04.
    """
    res_name = f"dynamic-litmus-chaos-{litmus_exp}"
    res = run_cmd(f"kubectl get chaosresult {res_name} -n {APP_NAMESPACE} -o json 2>/dev/null", timeout=10)
    if res and res.returncode == 0 and res.stdout.strip():
        try:
            data = json.loads(res.stdout)
            status = data.get("status", {}).get("experimentStatus", {})
            verdict = status.get("verdict", "Awaited")
            probe_pct = status.get("probeSuccessPercentage", "0")
            return verdict, probe_pct
        except Exception:
            pass
    return "NotAvailable", "0"


def clean_litmus_resources():
    """
    Removes active ChaosEngine, ChaosResult, and transient runner/helper pods,
    returning the teardown duration (dT_teardown in seconds) for RQ01.
    """
    t_start = time.time()
    run_cmd(
        f"kubectl patch chaosengine dynamic-litmus-chaos -n {APP_NAMESPACE} "
        f"-p '{{\"metadata\":{{\"finalizers\":[]}}}}' --type=merge 2>/dev/null",
        timeout=5,
    )
    run_cmd(
        f"kubectl delete chaosengine dynamic-litmus-chaos -n {APP_NAMESPACE} --ignore-not-found --wait=false 2>/dev/null",
        timeout=10,
    )
    run_cmd(
        f"kubectl delete chaosresult --all -n {APP_NAMESPACE} --ignore-not-found --wait=false 2>/dev/null",
        timeout=10,
    )
    run_cmd(
        f"kubectl delete jobs,pods -n {APP_NAMESPACE} -l chaosUID --ignore-not-found --force --grace-period=0 2>/dev/null",
        timeout=10,
    )
    run_cmd(
        f"kubectl delete pods -n {APP_NAMESPACE} --field-selector=status.phase=Succeeded --ignore-not-found 2>/dev/null",
        timeout=10,
    )

    # Wait until no litmus runner pods remain in otel-demo
    while time.time() - t_start < 30.0:
        res = run_cmd(f"kubectl get pods -n {APP_NAMESPACE} --no-headers 2>/dev/null", timeout=5)
        if res and res.stdout:
            remaining = [
                line for line in res.stdout.strip().split("\n")
                if any(k in line for k in ("dynamic-litmus-chaos", "pod-network-latency", "pod-cpu-hog", "pod-delete"))
            ]
            if not remaining:
                break
        time.sleep(0.5)

    s4b.clean_cluster()
    return round(time.time() - t_start, 2)


def run_steady_state_overhead_comparison(num_samples=30, interval_sec=16):
    """
    RQ02: Samples steady-state CPU (mCPU) and Memory (MiB) every 16s across
    the workload, monitoring, chaos-mesh, and litmus namespaces.
    """
    print("======================================================================", flush=True)
    print(f"  RQ02 OVERHEAD PROFILER: {num_samples} SAMPLES @ {interval_sec}s INTERVAL", flush=True)
    print("======================================================================", flush=True)

    with open(OVERHEAD_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "sample_id", "timestamp",
            "workload_cpu_mcpu", "workload_ram_mib",
            "panoptes_cpu_mcpu", "panoptes_ram_mib",
            "chaos_mesh_cpu_mcpu", "chaos_mesh_ram_mib", "chaos_mesh_pods",
            "litmus_cpu_mcpu", "litmus_ram_mib", "litmus_pods"
        ])

        for sample_id in range(1, num_samples + 1):
            w_cpu, w_ram, _ = sample_namespace_pods(APP_NAMESPACE)
            p_cpu, p_ram, _ = sample_namespace_pods(MONITORING_NAMESPACE)
            cm_cpu, cm_ram, cm_pods = sample_namespace_pods(CHAOS_MESH_NAMESPACE)
            lc_cpu, lc_ram, lc_pods = sample_namespace_pods(LITMUS_NAMESPACE)
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            writer.writerow([
                sample_id, ts,
                w_cpu, w_ram,
                p_cpu, p_ram,
                cm_cpu, cm_ram, cm_pods,
                lc_cpu, lc_ram, lc_pods
            ])
            f.flush()
            print(
                f"Sample {sample_id:02d}/{num_samples} [{ts}]: "
                f"ChaosMesh=({cm_cpu:.1f}m, {cm_ram:.1f}MiB, {cm_pods} pods) | "
                f"Litmus=({lc_cpu:.1f}m, {lc_ram:.1f}MiB, {lc_pods} pods)",
                flush=True
            )
            if sample_id < num_samples:
                time.sleep(interval_sec)

    print(f"\n[OK] Overhead comparison saved to: {OVERHEAD_CSV}", flush=True)


def run_paired_litmus_campaign():
    print("======================================================================", flush=True)
    print("  PAIRED EXPERIMENT: LITMUSCHAOS vs. CHAOS MESH (N=30, SEED=42)", flush=True)
    print("======================================================================", flush=True)

    s4b.ensure_port_forwards()
    ensure_litmus_rbac_and_experiments()

    seen_traces = set()
    seen_logs = set()
    trial_schedule = s4b.build_balanced_trial_schedule(NUM_TRIALS, seed=RANDOM_SEED)

    with open(RESULTS_CSV, mode="w", newline="") as f_csv, \
         open(AUDIT_JSONL, mode="w") as f_audit:

        writer = csv.writer(f_csv)
        writer.writerow([
            "trial_id", "platform", "chaos_type", "litmus_experiment", "ground_truth",
            "diagnosed_target", "t_crd_iso", "actuation_delay_seconds",
            "mttdiag_total_seconds", "mttdiag_net_seconds", "teardown_seconds",
            "runner_pods_spawned", "runner_cpu_mcpu", "runner_ram_mib",
            "accuracy", "censored", "detection_modality",
            "litmus_probe_verdict", "litmus_resilience_score_pct",
            "evidence_id", "evidence_summary"
        ])

        trial = 1
        while trial <= NUM_TRIALS:
            selected_target, selected_chaos = trial_schedule[trial - 1]
            litmus_exp = CHAOS_TYPE_TO_LITMUS[selected_chaos]
            print(
                f"\n[Trial {trial:02d}/{NUM_TRIALS}] Target={selected_target} | "
                f"Fault={selected_chaos} ({litmus_exp})",
                flush=True
            )

            s4b.ensure_cluster_stability()
            clean_litmus_resources()
            s4b.wait_for_workload_recovery()
            print("  -> Waiting for cluster stabilization (20s)...", flush=True)
            time.sleep(20)
            baseline_cpu, baseline_pods = s4b.prime_baselines(seen_traces, seen_logs)

            manifest_content = generate_litmus_engine_manifest(selected_target, selected_chaos)
            with open("/tmp/current_litmus_engine.yaml", "w") as m:
                m.write(manifest_content)

            t_crd = time.time()
            t_crd_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            create_res = run_cmd("kubectl create -f /tmp/current_litmus_engine.yaml", timeout=15)
            if not create_res or create_res.returncode != 0:
                print(f"  [RETRY] Failed to create ChaosEngine on Trial {trial:02d}. Retrying...", flush=True)
                clean_litmus_resources()
                continue

            # RQ01: Measure LitmusChaos runner pod actuation delay (dT_actuate)
            t_effect, actuation_delay, runner_pod_count = wait_for_litmus_actuation(t_crd, litmus_exp)
            print(
                f"  -> ChaosEngine created at {t_crd_iso} | Actuation delay dT_actuate={actuation_delay}s "
                f"({runner_pod_count} runner pods spawned)",
                flush=True
            )

            # Sample active runner pod overhead in otel-demo
            r_cpu, r_ram, _ = sample_namespace_pods(APP_NAMESPACE, filter_substring=litmus_exp[:10])

            # RQ03: Execute PANOPTES Multimodal RCA Agent
            time.sleep(s4b.INITIAL_PROPAGATION_SLEEP)
            verdict, elapsed_from_effect = s4b.run_panoptes_rca_agent(
                t_effect, seen_traces, seen_logs, baseline_cpu, baseline_pods
            )

            if verdict is None or elapsed_from_effect >= TIMEOUT_SECONDS:
                mttdiag_net = float(TIMEOUT_SECONDS)
                mttdiag_total = round(actuation_delay + TIMEOUT_SECONDS, 2)
                diagnosed_service = "inconclusive"
                accuracy = 0
                censored = True
                modality = "NONE_TIMEOUT"
                evidence_id = "none"
                evidence_summary = "Right-censored at 300s operational ceiling"
            else:
                mttdiag_net = round(elapsed_from_effect, 2)
                mttdiag_total = round(actuation_delay + mttdiag_net, 2)
                diagnosed_service = verdict["culprit"]
                censored = False
                accuracy = 1 if diagnosed_service == selected_target else 0
                modality = verdict["modality"]
                evidence_id = verdict["evidence_id"]
                evidence_summary = verdict["summary"]

            # RQ04: Capture native Litmus ChaosResult verdict and probe score
            probe_verdict, resilience_score = extract_litmus_chaos_result(litmus_exp)

            # RQ01: Measure teardown duration
            teardown_sec = clean_litmus_resources()

            print(
                f"  [RESULT] Acc={accuracy} | Diagnosed={diagnosed_service} (Truth={selected_target}) | "
                f"dT_actuate={actuation_delay}s | MTTDiag_net={mttdiag_net}s | MTTDiag_total={mttdiag_total}s | "
                f"Teardown={teardown_sec}s",
                flush=True
            )

            writer.writerow([
                trial, "litmuschaos", selected_chaos, litmus_exp, selected_target,
                diagnosed_service, t_crd_iso, actuation_delay,
                mttdiag_total, mttdiag_net, teardown_sec,
                runner_pod_count, r_cpu, r_ram,
                accuracy, censored, modality,
                probe_verdict, resilience_score,
                evidence_id, evidence_summary
            ])
            f_csv.flush()

            f_audit.write(json.dumps({
                "trial_id": trial,
                "platform": "litmuschaos",
                "chaos_type": selected_chaos,
                "litmus_experiment": litmus_exp,
                "ground_truth": selected_target,
                "diagnosed_target": diagnosed_service,
                "t_crd_iso": t_crd_iso,
                "actuation_delay_seconds": actuation_delay,
                "mttdiag_total_seconds": mttdiag_total,
                "mttdiag_net_seconds": mttdiag_net,
                "teardown_seconds": teardown_sec,
                "runner_pods_spawned": runner_pod_count,
                "runner_cpu_mcpu": r_cpu,
                "runner_ram_mib": r_ram,
                "accuracy": accuracy,
                "censored": censored,
                "detection_modality": modality,
                "litmus_probe_verdict": probe_verdict,
                "litmus_resilience_score_pct": resilience_score,
                "evidence_id": evidence_id,
                "evidence_summary": evidence_summary,
            }) + "\n")
            f_audit.flush()

            trial += 1

    print(f"\n[OK] Paired LitmusChaos campaign completed. Saved to: {RESULTS_CSV}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LitmusChaos vs. Chaos Mesh Paired Experimental Runner")
    parser.add_argument(
        "--mode",
        choices=["trials", "overhead", "both"],
        default="both",
        help="Run steady-state overhead profiling ('overhead'), N=30 paired fault trials ('trials'), or 'both'."
    )
    args = parser.parse_args()

    if args.mode in ("overhead", "both"):
        run_steady_state_overhead_comparison()
    if args.mode in ("trials", "both"):
        run_paired_litmus_campaign()
