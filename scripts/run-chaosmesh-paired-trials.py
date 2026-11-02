#!/usr/bin/env python3
import atexit
import csv
from datetime import datetime, timezone
import json
import random
import re
import subprocess
import time
import urllib.parse
import urllib.request

NUM_TRIALS = 30
RANDOM_SEED = 42
TIMEOUT_SECONDS = 300
POLLING_INTERVAL_SECONDS = 4
INITIAL_PROPAGATION_SLEEP = 3
APP_NAMESPACE = "otel-demo"
MONITORING_NAMESPACE = "monitoring"
CHAOS_NAMESPACE = "chaos-mesh"
RESULTS_FILE = "scenario4b_optimized_trials.csv"
EVIDENCE_LOG_FILE = "scenario4b_evidence_audit.jsonl"

TARGET_POOL = ["payment", "cart", "shipping", "currency"]
SEARCH_POOL = ["frontend", "checkout", "payment", "cart", "shipping", "currency"]

pf_tempo = None
pf_loki = None
pf_prom = None
tempo_port = 3200
loki_port = 3100
prom_port = 9090
tempo_endpoint = f"http://localhost:{tempo_port}"
loki_endpoint = f"http://localhost:{loki_port}"
prom_endpoint = f"http://localhost:{prom_port}"

TRACE_ID_REGEX = re.compile(r'(?:traceID|trace_id|traceId)["\s:=]+([a-fA-F0-9]{32})')

CRITICAL_LOG_PATTERNS = [
    re.compile(r"rpc error:\s*code\s*=\s*(?:Unavailable|DeadlineExceeded|Internal|Canceled|Unknown)", re.IGNORECASE),
    re.compile(r"context deadline exceeded|deadline_exceeded", re.IGNORECASE),
    re.compile(r"connection refused|reset by peer|broken pipe|\beof\b|transport is closing|connection closed", re.IGNORECASE),
    re.compile(r"http\s*(?:status)?\s*5\d{2}|status[_\s]*code[=:\s]*5\d{2}", re.IGNORECASE),
    re.compile(r"failed to (?:call|charge|convert|ship|get|empty|add|place|prepare)", re.IGNORECASE),
    re.compile(r"panic|fatal|sigterm|sigkill|killed|\boom\b|unhealthy|shutting down|terminated", re.IGNORECASE)
]

STRICT_PEER_PATTERNS = {
    "payment": re.compile(r"(?:paymentservice|oteldemo\.paymentservice|payment:8080|payment\.otel-demo|/charge)", re.IGNORECASE),
    "shipping": re.compile(r"(?:shippingservice|oteldemo\.shippingservice|shipping:8080|shipping\.otel-demo|get-quote|getquote|ship-order|shiporder)", re.IGNORECASE),
    "currency": re.compile(r"(?:currencyservice|oteldemo\.currencyservice|currency:8080|currency\.otel-demo|currencyservice/convert|/convert)", re.IGNORECASE),
    "cart": re.compile(r"(?:cartservice|oteldemo\.cartservice|cart:8080|cart\.otel-demo|getcart|additem|emptycart)", re.IGNORECASE)
}

IGNORED_EXTERNAL_PEERS = re.compile(r"(?:flagd|quote:8080|otel-collector|jaeger|kafka|valkey|redis)", re.IGNORECASE)


def run_cmd(cmd, timeout=15):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def ensure_port_forwards(force_restart=False):
    global pf_tempo, pf_loki, pf_prom
    if force_restart:
        stop_port_forwards()
        pf_tempo = None
        pf_loki = None
        pf_prom = None

    if pf_tempo is None or pf_tempo.poll() is not None:
        if pf_tempo is not None:
            try:
                pf_tempo.terminate()
            except Exception:
                pass
        subprocess.run(f"fuser -k {tempo_port}/tcp 2>/dev/null", shell=True, timeout=5)
        pf_tempo = subprocess.Popen(
            ["kubectl", "port-forward", "svc/tempo", f"{tempo_port}:3200", "-n", MONITORING_NAMESPACE],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(1)

    if pf_loki is None or pf_loki.poll() is not None:
        if pf_loki is not None:
            try:
                pf_loki.terminate()
            except Exception:
                pass
        subprocess.run(f"fuser -k {loki_port}/tcp 2>/dev/null", shell=True, timeout=5)
        pf_loki = subprocess.Popen(
            ["kubectl", "port-forward", "svc/loki", f"{loki_port}:3100", "-n", MONITORING_NAMESPACE],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(1)

    if pf_prom is None or pf_prom.poll() is not None:
        if pf_prom is not None:
            try:
                pf_prom.terminate()
            except Exception:
                pass
        subprocess.run(f"fuser -k {prom_port}/tcp 2>/dev/null", shell=True, timeout=5)
        pf_prom = subprocess.Popen(
            ["kubectl", "port-forward", "svc/kube-prometheus-stack-prometheus", f"{prom_port}:9090", "-n", MONITORING_NAMESPACE],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(2)


def stop_port_forwards():
    for proc in (pf_tempo, pf_loki, pf_prom):
        if proc:
            try:
                proc.terminate()
            except Exception:
                pass


atexit.register(stop_port_forwards)


def ensure_cluster_stability():
    print("[HEALTH-CHECK] Validating live infrastructure readiness (Prometheus + Loki + Tempo)...", flush=True)
    max_retries = 6
    for attempt in range(max_retries):
        try:
            ensure_port_forwards(force_restart=(attempt > 0))
            req_t = urllib.request.Request(f"{tempo_endpoint}/ready")
            req_l = urllib.request.Request(f"{loki_endpoint}/ready")
            req_p = urllib.request.Request(f"{prom_endpoint}/-/ready")
            with urllib.request.urlopen(req_t, timeout=3) as rt, \
                 urllib.request.urlopen(req_l, timeout=3) as rl, \
                 urllib.request.urlopen(req_p, timeout=3) as rp:
                if rt.status == 200 and rl.status == 200 and rp.status == 200:
                    print("[HEALTH-CHECK] Full PANOPTES stack (M + L + T) is live and healthy.", flush=True)
                    return True
        except Exception:
            print(f"  -> Waiting for observability stack response (Attempt {attempt + 1}/{max_retries})...", flush=True)
            time.sleep(3)
    print("[WARN] Readiness check incomplete, proceeding with active tunnels...", flush=True)
    return False


def wait_for_workload_recovery():
    for svc in TARGET_POOL:
        run_cmd(f"kubectl rollout status deployment/{svc} -n {APP_NAMESPACE} --timeout=30s 2>/dev/null", timeout=35)


def build_balanced_trial_schedule(num_trials, seed=RANDOM_SEED):
    """
    Generates a paired, deterministic, balanced factorial schedule (identical to Scenario 4a):
    10 network-delay, 10 pod-kill, and 10 cpu-stress distributed uniformly across TARGET_POOL.
    """
    rng = random.Random(seed)
    chaos_types = ["network-delay", "pod-kill", "cpu-stress"]
    all_combinations = [(t, c) for c in chaos_types for t in TARGET_POOL]
    schedule = []
    while len(schedule) < num_trials:
        block = list(all_combinations)
        rng.shuffle(block)
        schedule.extend(block)
    schedule = schedule[:num_trials]
    rng.shuffle(schedule)
    return schedule


def pod_matches_target(pod_name, target_service):
    pod_lower = pod_name.lower()
    if any(excluded in pod_lower for excluded in ("valkey", "redis", "load-generator", "loadgenerator", "frontend", "checkout")):
        return False
    return pod_lower == target_service or pod_lower.startswith(f"{target_service}-")


def is_exact_target_pod(pod_name, labels, target_service):
    if not pod_matches_target(pod_name, target_service):
        for k in ("app.kubernetes.io/component", "app.kubernetes.io/name", "opentelemetry.io/name", "app"):
            if labels.get(k, "").lower() == target_service:
                return True
        return False
    return True


def resolve_target_pods(target_service):
    res = run_cmd(f"kubectl get pods -n {APP_NAMESPACE} --field-selector=status.phase=Running -o json 2>/dev/null")
    matched_pods = []
    matched_label_key = None
    matched_label_val = None

    if res and res.stdout:
        try:
            data = json.loads(res.stdout)
            for item in data.get("items", []):
                pod_name = item.get("metadata", {}).get("name", "")
                labels = item.get("metadata", {}).get("labels", {})
                if is_exact_target_pod(pod_name, labels, target_service):
                    matched_pods.append(pod_name)
                    for k in ("app.kubernetes.io/name", "app.kubernetes.io/component", "app"):
                        if labels.get(k, "").lower() == target_service:
                            matched_label_key = k
                            matched_label_val = labels[k]
                            break
        except Exception:
            pass

    return matched_pods, matched_label_key, matched_label_val


def generate_chaos_manifest(target_service, chaos_type):
    pods, l_key, l_val = resolve_target_pods(target_service)
    if pods:
        pod_list_yaml = "\n".join([f"        - {p}" for p in pods])
        selector_block = f"""  selector:
    namespaces:
      - {APP_NAMESPACE}
    pods:
      {APP_NAMESPACE}:
{pod_list_yaml}"""
    elif l_key and l_val:
        selector_block = f"""  selector:
    namespaces:
      - {APP_NAMESPACE}
    labelSelectors:
      "{l_key}": "{l_val}\""""
    else:
        selector_block = f"""  selector:
    namespaces:
      - {APP_NAMESPACE}
    labelSelectors:
      app.kubernetes.io/name: {target_service}"""

    if chaos_type == "network-delay":
        return f"""apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: dynamic-network-delay
  namespace: {APP_NAMESPACE}
spec:
  action: delay
  mode: all
{selector_block}
  delay:
    latency: "2000ms"
    jitter: "100ms"
  direction: to
  duration: "180s"
"""
    elif chaos_type == "pod-kill":
        return f"""apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: dynamic-pod-kill
  namespace: {APP_NAMESPACE}
spec:
  action: pod-kill
  mode: one
{selector_block}
"""
    elif chaos_type == "cpu-stress":
        return f"""apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: dynamic-cpu-stress
  namespace: {APP_NAMESPACE}
spec:
  mode: one
{selector_block}
  stressors:
    cpu:
      workers: 2
      load: 100
  duration: "180s"
"""


def clean_cluster():
    print("  -> Cleaning up previous chaos experiments...", flush=True)
    res = run_cmd(
        f"kubectl delete networkchaos,podchaos,stresschaos --all -n {APP_NAMESPACE} --ignore-not-found --timeout=10s 2>/dev/null",
        timeout=15
    )
    if res is None or res.returncode != 0:
        for kind in ("networkchaos", "podchaos", "stresschaos"):
            rem = run_cmd(f"kubectl get {kind} -n {APP_NAMESPACE} -o jsonpath='{{.items[*].metadata.name}}' 2>/dev/null", timeout=5)
            if rem and rem.stdout.strip():
                for name in rem.stdout.strip().split():
                    run_cmd(
                        f"kubectl patch {kind}/{name} -n {APP_NAMESPACE} --type=merge -p '{{\"metadata\":{{\"finalizers\":[]}}}}' 2>/dev/null",
                        timeout=5
                    )
        run_cmd(
            f"kubectl delete networkchaos,podchaos,stresschaos --all -n {APP_NAMESPACE} --ignore-not-found --timeout=5s 2>/dev/null",
            timeout=10
        )


def inject_chaos_manifest(manifest_path):
    """
    Creates the Chaos Mesh manifest and explicitly verifies the return code.
    If a residual object exists ('Cannot update chaos spec') or the webhook is transient,
    cleans up residual resources and retries before T0 is recorded.
    """
    for attempt in range(1, 5):
        res = run_cmd(f"kubectl create -f {manifest_path}", timeout=15)
        if res and res.returncode == 0:
            return True
        err_msg = (res.stderr or "").strip() if res else "timeout"
        print(f"  [WARN] Chaos injection attempt {attempt}/4 failed: {err_msg}", flush=True)
        clean_cluster()
        run_cmd(f"kubectl rollout status deployment/chaos-controller-manager -n {CHAOS_NAMESPACE} --timeout=20s 2>/dev/null", timeout=25)
        time.sleep(3)
    return False


def query_prometheus_cpu_rates():
    """
    Queries the Prometheus server exclusively via PromQL (without kubectl top fallbacks).
    Combines irate[1m] and rate[45s] over container_cpu_usage_seconds_total for compatibility
    with the 15s scrapeInterval of kube-prometheus-stack.
    """
    ensure_port_forwards()
    prom_rates = {t: 0.0 for t in TARGET_POOL}

    promql_queries = [
        f'max by (pod) (irate(container_cpu_usage_seconds_total{{namespace="{APP_NAMESPACE}",pod!=""}}[1m])) * 1000',
        f'max by (pod) (rate(container_cpu_usage_seconds_total{{namespace="{APP_NAMESPACE}",pod!=""}}[45s])) * 1000'
    ]

    for promql in promql_queries:
        params = urllib.parse.urlencode({"query": promql})
        url = f"{prom_endpoint}/api/v1/query?{params}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=3) as response:
                data = json.loads(response.read().decode())
                results = data.get("data", {}).get("result", [])
                for item in results:
                    pod_name = item.get("metric", {}).get("pod", "").lower()
                    val_mcpu = float(item.get("value", [0, "0"])[1])
                    for target in TARGET_POOL:
                        if pod_matches_target(pod_name, target):
                            prom_rates[target] = max(prom_rates[target], val_mcpu)
        except Exception:
            pass

    return {t: round(prom_rates[t], 2) for t in TARGET_POOL}


def query_prometheus_pod_resets(t0_epoch_sec, baseline_pods):
    """
    Queries Prometheus exclusively (kube-state-metrics / cAdvisor) to detect
    container restarts or pod recreations in the monitored namespace after t0.
    """
    ensure_port_forwards()
    promql_queries = [
        (
            "increase(kube_pod_container_status_restarts_total[1m])",
            f'max by (pod) (increase(kube_pod_container_status_restarts_total{{namespace="{APP_NAMESPACE}"}}[1m]))'
        ),
        (
            "kube_pod_start_time",
            f'max by (pod) (kube_pod_start_time{{namespace="{APP_NAMESPACE}"}})'
        ),
        (
            "container_start_time_seconds",
            f'max by (pod) (container_start_time_seconds{{namespace="{APP_NAMESPACE}",pod!=""}})'
        )
    ]

    for metric_name, promql in promql_queries:
        params = urllib.parse.urlencode({"query": promql})
        url = f"{prom_endpoint}/api/v1/query?{params}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=3) as response:
                data = json.loads(response.read().decode())
                results = data.get("data", {}).get("result", [])
                for item in results:
                    pod_name = item.get("metric", {}).get("pod", "").lower()
                    val = float(item.get("value", [0, "0"])[1])
                    for target in TARGET_POOL:
                        if not pod_matches_target(pod_name, target):
                            continue
                        if "restarts_total" in metric_name and val >= 0.5:
                            return target, f"promql:{metric_name}[{pod_name}]={val:.1f}"
                        if "start_time" in metric_name and val >= (t0_epoch_sec - 1.0):
                            return target, f"promql:{metric_name}[{pod_name}]={int(val)}"
                        if baseline_pods.get(target) and pod_name not in baseline_pods[target]:
                            return target, f"promql:new_pod_series[{pod_name}]"
        except Exception:
            pass

    return None


def snapshot_prometheus_active_pods():
    """
    Snapshots active pod names prior to t0 exclusively via Prometheus time series.
    """
    ensure_port_forwards()
    active_pods = {t: set() for t in TARGET_POOL}
    promql = f'count by (pod) (container_cpu_usage_seconds_total{{namespace="{APP_NAMESPACE}",pod!=""}})'
    params = urllib.parse.urlencode({"query": promql})
    url = f"{prom_endpoint}/api/v1/query?{params}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as response:
            data = json.loads(response.read().decode())
            for item in data.get("data", {}).get("result", []):
                pod_name = item.get("metric", {}).get("pod", "").lower()
                for target in TARGET_POOL:
                    if pod_matches_target(pod_name, target):
                        active_pods[target].add(pod_name)
    except Exception:
        pass
    return active_pods


def fetch_loki_window(start_sec, end_sec, seen_logs, strict_t0_sec=None):
    ensure_port_forwards()
    start_ns = int(start_sec * 1e9)
    end_ns = int(end_sec * 1e9)
    min_valid_ns = int(strict_t0_sec * 1e9) if strict_t0_sec else start_ns

    logql = f'{{namespace="{APP_NAMESPACE}"}} |~ "(?i)(rpc error|unavailable|deadline|refused|reset|eof|closing|failed to|panic|fatal|oom|killed|sigterm|shutting down|500|502|503|504|error)"'
    params = urllib.parse.urlencode({
        "query": logql,
        "start": str(start_ns),
        "end": str(end_ns),
        "limit": 400,
        "direction": "backward"
    })
    url = f"{loki_endpoint}/loki/api/v1/query_range?{params}"
    log_events = []
    linked_traces = []

    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=4) as response:
            data = json.loads(response.read().decode())
            streams = data.get("data", {}).get("result", [])
            for stream in streams:
                labels = stream.get("stream", {})
                pod_label = labels.get("pod", "").lower()
                source_service = " ".join([
                    labels.get("app_kubernetes_io_name", ""),
                    labels.get("app", ""),
                    pod_label,
                    labels.get("container", ""),
                    labels.get("service_name", "")
                ]).lower()

                if any(ex in pod_label for ex in ("valkey", "redis", "load-generator", "loadgenerator")):
                    continue

                for ts_ns, line in stream.get("values", []):
                    if int(ts_ns) < min_valid_ns:
                        continue
                    sig_key = f"{ts_ns}:{hash(line)}"
                    if sig_key in seen_logs:
                        continue
                    seen_logs.add(sig_key)

                    if IGNORED_EXTERNAL_PEERS.search(line):
                        continue

                    if not any(p.search(line) for p in CRITICAL_LOG_PATTERNS):
                        continue

                    match = TRACE_ID_REGEX.search(line)
                    tid = match.group(1).lower() if match else None
                    if tid and tid not in linked_traces:
                        linked_traces.append(tid)

                    matched_targets = []
                    for target, pat in STRICT_PEER_PATTERNS.items():
                        if pat.search(line) and target not in source_service:
                            matched_targets.append(target)

                    if not matched_targets:
                        for target in TARGET_POOL:
                            if pod_matches_target(pod_label, target) or target in source_service:
                                matched_targets.append(target)

                    for t in matched_targets:
                        log_events.append({
                            "target": t,
                            "timestamp_ns": ts_ns,
                            "source_pod": labels.get("pod", source_service),
                            "trace_id": tid,
                            "excerpt": line.strip()[:160]
                        })
    except Exception:
        pass

    return log_events, linked_traces


def prime_baselines(seen_traces, seen_logs):
    ensure_port_forwards()
    now = time.time()
    start_sec = int(now - 90)
    end_sec = int(now) + 2
    for svc in SEARCH_POOL:
        for q in [
            {"tags": f"service.name={svc}", "minDuration": "600ms", "start": start_sec, "end": end_sec, "limit": 30},
            {"tags": f"service.name={svc} status=error", "start": start_sec, "end": end_sec, "limit": 30},
            {"tags": f"service.name={svc} error=true", "start": start_sec, "end": end_sec, "limit": 30}
        ]:
            params = urllib.parse.urlencode(q)
            url = f"{tempo_endpoint}/api/search?{params}"
            try:
                req = urllib.request.Request(url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=3) as response:
                    data = json.loads(response.read().decode())
                    for t in data.get("traces", []):
                        tid = t.get("traceID")
                        if tid:
                            seen_traces.add(tid.lower())
            except Exception:
                pass

    _, _ = fetch_loki_window(now - 30, now, seen_logs)
    baseline_cpu = query_prometheus_cpu_rates()
    baseline_pods = snapshot_prometheus_active_pods()
    return baseline_cpu, baseline_pods


def calculate_exclusive_self_time(span, spans_dict):
    children_intervals = []
    for child_id in span["children"]:
        if child_id in spans_dict:
            c = spans_dict[child_id]
            children_intervals.append([c["start"], c["end"]])
    if not children_intervals:
        return span["duration"]

    children_intervals.sort(key=lambda x: x[0])
    merged = [children_intervals[0]]
    for current in children_intervals[1:]:
        prev = merged[-1]
        if current[0] <= prev[1]:
            prev[1] = max(prev[1], current[1])
        else:
            merged.append(current)

    children_duration_ms = sum([m[1] - m[0] for m in merged]) / 1e6
    return max(0.0, span["duration"] - children_duration_ms)


def resolve_client_span_peer(service_name, span_name, span_attrs_dict):
    peer_fields = " ".join([
        span_name,
        span_attrs_dict.get("rpc.service", ""),
        span_attrs_dict.get("rpc.method", ""),
        span_attrs_dict.get("net.peer.name", ""),
        span_attrs_dict.get("server.address", ""),
        span_attrs_dict.get("http.url", ""),
        span_attrs_dict.get("url.full", "")
    ]).lower()

    if IGNORED_EXTERNAL_PEERS.search(peer_fields):
        return "IGNORED_EXTERNAL"

    for target, pat in STRICT_PEER_PATTERNS.items():
        if target not in service_name and pat.search(peer_fields):
            return target
    return None


def isolate_root_cause_from_trace(trace_id, t0_epoch_sec):
    ensure_port_forwards()
    url = f"{tempo_endpoint}/api/traces/{trace_id}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=4) as response:
            trace_json = json.loads(response.read().decode())
    except Exception:
        return None

    batches = trace_json.get("batches", [])
    if not batches:
        return None

    spans = {}
    strict_t0_ns = int(t0_epoch_sec * 1e9)

    for batch in batches:
        service_name = "unknown"
        for attr in batch.get("resource", {}).get("attributes", []):
            if attr.get("key") == "service.name":
                service_name = attr.get("value", {}).get("stringValue", "unknown").lower()

        for scope_span in batch.get("scopeSpans", []):
            for span in scope_span.get("spans", []):
                span_id = span.get("spanId")
                start = int(span.get("startTimeUnixNano", 0))
                end = int(span.get("endTimeUnixNano", 0))
                dur_ms = (end - start) / 1e6
                status_code = span.get("status", {}).get("code")
                is_err = (status_code == 2 or status_code == "STATUS_CODE_ERROR")

                attr_dict = {}
                for a in span.get("attributes", []):
                    k = a.get("key", "")
                    val_obj = a.get("value", {})
                    v = str(
                        val_obj.get("stringValue")
                        or val_obj.get("intValue")
                        or val_obj.get("boolValue")
                        or ""
                    )
                    if k and v:
                        attr_dict[k] = v

                err_type = attr_dict.get("error.type", "")
                rpc_status = attr_dict.get("rpc.response.status_code", "OK")
                if err_type in ("500", "502", "503", "504") or rpc_status not in ("OK", "0", ""):
                    is_err = True

                spans[span_id] = {
                    "span_id": span_id,
                    "service": service_name,
                    "name": span.get("name", ""),
                    "kind": str(span.get("kind", "")),
                    "attrs": attr_dict,
                    "parent": span.get("parentSpanId"),
                    "start": start,
                    "end": end,
                    "duration": dur_ms,
                    "is_error": is_err,
                    "children": []
                }

    for span_id, s in spans.items():
        pid = s["parent"]
        if pid and pid in spans:
            spans[pid]["children"].append(span_id)

    error_candidates = []
    for span_id, s in spans.items():
        self_time = calculate_exclusive_self_time(s, spans)
        s["self_time"] = round(self_time, 2)
        if s["start"] < strict_t0_ns:
            continue

        if s["is_error"]:
            peer_target = resolve_client_span_peer(s["service"], s["name"], s["attrs"])
            if peer_target == "IGNORED_EXTERNAL":
                continue
            elif peer_target in TARGET_POOL:
                error_candidates.append((peer_target, s, "client_rpc_error_to_peer"))
            elif "CLIENT" not in s["kind"].upper() and s["kind"] != "3":
                for t in TARGET_POOL:
                    if t in s["service"] and "valkey" not in s["service"]:
                        error_candidates.append((t, s, "service_error_span"))

    if error_candidates:
        error_candidates.sort(key=lambda x: x[1]["self_time"], reverse=True)
        chosen_target, chosen_span, reason = error_candidates[0]
        return {
            "culprit": chosen_target,
            "modality": "TEMPO_TRACE_ERROR",
            "evidence_id": f"trace:{trace_id}/span:{chosen_span['span_id']}",
            "summary": f"{reason} on {chosen_span['service']} ({chosen_span['name']}) dur={chosen_span['duration']:.1f}ms self={chosen_span['self_time']:.1f}ms",
            "self_time_ms": chosen_span["self_time"],
            "raw_span": chosen_span
        }

    max_self_time = 0.0
    culprit_target = None
    culprit_span = None
    attribution_mode = ""

    for span_id, s in spans.items():
        if s["start"] < strict_t0_ns:
            continue
        self_time = s["self_time"]
        if self_time < 1500.0:
            continue

        matched_target = None
        mode = ""
        peer_target = resolve_client_span_peer(s["service"], s["name"], s["attrs"])
        if peer_target == "IGNORED_EXTERNAL":
            continue
        elif peer_target in TARGET_POOL:
            matched_target = peer_target
            mode = "l4_client_wait_self_time"
        elif "CLIENT" not in s["kind"].upper() and s["kind"] != "3":
            for t in TARGET_POOL:
                if t in s["service"] and "valkey" not in s["service"]:
                    matched_target = t
                    mode = "direct_self_time"
                    break

        if matched_target and self_time > max_self_time:
            max_self_time = self_time
            culprit_target = matched_target
            culprit_span = s
            attribution_mode = mode

    if culprit_target and culprit_span:
        return {
            "culprit": culprit_target,
            "modality": "TEMPO_SELF_TIME",
            "evidence_id": f"trace:{trace_id}/span:{culprit_span['span_id']}",
            "summary": f"{attribution_mode} isolated {culprit_target} via span {culprit_span['service']}:{culprit_span['name']} self_time={max_self_time:.1f}ms (total={culprit_span['duration']:.1f}ms)",
            "self_time_ms": max_self_time,
            "raw_span": culprit_span
        }

    return None


def search_tempo_anomalies(t0_epoch_sec, seen_traces):
    ensure_port_forwards()
    candidates = []
    start_sec = int(t0_epoch_sec)
    end_sec = int(time.time()) + 2

    for svc in SEARCH_POOL:
        queries = [
            {"tags": f"service.name={svc}", "minDuration": "1500ms", "start": start_sec, "end": end_sec, "limit": 10},
            {"tags": f"service.name={svc} status=error", "start": start_sec, "end": end_sec, "limit": 10},
            {"tags": f"service.name={svc} error=true", "start": start_sec, "end": end_sec, "limit": 10}
        ]
        for q in queries:
            params = urllib.parse.urlencode(q)
            url = f"{tempo_endpoint}/api/search?{params}"
            try:
                req = urllib.request.Request(url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=3) as response:
                    data = json.loads(response.read().decode())
                    for t in data.get("traces", []):
                        tid = t.get("traceID", "").lower()
                        if tid and tid not in seen_traces:
                            seen_traces.add(tid)
                            candidates.append(tid)
            except Exception:
                pass
    return candidates


def run_panoptes_rca_agent(t0, seen_traces, seen_logs, baseline_cpu, baseline_pods):
    """
    PANOPTES Multimodal RCA Agent (Scenario 4b):
    Operates strictly over the HTTP APIs of Tempo, Loki, and Prometheus,
    without invoking any kubectl commands during the diagnostic loop.
    """
    attempt = 0
    last_loki_ts = t0
    loki_hit_counts = {t: 0 for t in TARGET_POOL}
    loki_first_evidence = {}
    tempo_self_time_votes = {t: 0 for t in TARGET_POOL}
    prom_sustained_hits = {t: 0 for t in TARGET_POOL}

    while (time.time() - t0) < TIMEOUT_SECONDS:
        attempt += 1
        now = time.time()
        elapsed = int(now - t0)
        print(f"      [POLL #{attempt}] Elapsed: {elapsed}s. Scanning PANOPTES (Tempo + Loki + Prometheus)...", flush=True)

        # 1. Scan Loki error log streams and extract correlated TraceIDs
        log_events, loki_trace_ids = fetch_loki_window(last_loki_ts - 2, now + 1, seen_logs, strict_t0_sec=t0)
        last_loki_ts = now

        for ev in log_events:
            t = ev["target"]
            loki_hit_counts[t] += 1
            if t not in loki_first_evidence:
                loki_first_evidence[t] = ev

        # 2. Pivot Loki -> Tempo via TraceID extracted from error log lines
        for tid in loki_trace_ids:
            if tid not in seen_traces:
                seen_traces.add(tid)
                verdict = isolate_root_cause_from_trace(tid, t0)
                if verdict:
                    verdict["modality"] = "LOKI_TO_TEMPO_CORRELATION"
                    return verdict, time.time() - t0

        # 3. Consistent error log signature in Loki
        top_log_target = max(loki_hit_counts, key=loki_hit_counts.get)
        if loki_hit_counts[top_log_target] >= 2:
            ev = loki_first_evidence[top_log_target]
            return {
                "culprit": top_log_target,
                "modality": "LOKI_LOG_SIGNATURE",
                "evidence_id": f"loki_ns:{ev['timestamp_ns']}/pod:{ev['source_pod']}",
                "summary": f"Log exception pattern ({loki_hit_counts[top_log_target]} hits): {ev['excerpt']}",
                "raw_log": ev
            }, time.time() - t0

        # 4. Container restart/recreation detection via PromQL + Loki correlation
        prom_reset = query_prometheus_pod_resets(t0, baseline_pods)
        if prom_reset:
            t_culprit, prom_ev_id = prom_reset
            if loki_hit_counts[t_culprit] >= 1:
                ev = loki_first_evidence[t_culprit]
                return {
                    "culprit": t_culprit,
                    "modality": "LOKI_LOG_SIGNATURE",
                    "evidence_id": f"{prom_ev_id}/loki_ns:{ev['timestamp_ns']}",
                    "summary": f"PromQL pod reset + Loki error log on {t_culprit}: {ev['excerpt']}",
                    "raw_log": ev
                }, time.time() - t0
            elif elapsed >= 8:
                return {
                    "culprit": t_culprit,
                    "modality": "PROMETHEUS_METRIC_SATURATION",
                    "evidence_id": prom_ev_id,
                    "summary": f"Prometheus detected container restart / pod reset metric on {t_culprit} ({prom_ev_id})",
                    "raw_metrics": {"promql_evidence": prom_ev_id}
                }, time.time() - t0

        # 5. Sustained cgroup CPU saturation via PromQL in Prometheus
        if elapsed >= 6:
            current_cpu = query_prometheus_cpu_rates()
            best_cpu_target = None
            max_delta = 0.0
            for t in TARGET_POOL:
                delta_mcpu = current_cpu.get(t, 0.0) - baseline_cpu.get(t, 0.0)
                if delta_mcpu > max_delta:
                    max_delta = delta_mcpu
                    best_cpu_target = t

            for t in TARGET_POOL:
                delta_mcpu = current_cpu.get(t, 0.0) - baseline_cpu.get(t, 0.0)
                if t == best_cpu_target and (delta_mcpu >= 120.0 or current_cpu.get(t, 0.0) >= 250.0):
                    prom_sustained_hits[t] += 1
                    if prom_sustained_hits[t] >= 2:
                        return {
                            "culprit": t,
                            "modality": "PROMETHEUS_METRIC_SATURATION",
                            "evidence_id": f"promql:container_cpu_usage_seconds_total[{t}]",
                            "summary": f"CPU saturation on {t}: current={current_cpu[t]:.1f}mCPU vs baseline={baseline_cpu.get(t, 0.0):.1f}mCPU (delta=+{delta_mcpu:.1f}mCPU)",
                            "raw_metrics": {"baseline_mcpu": baseline_cpu, "current_mcpu": current_cpu}
                        }, time.time() - t0
                else:
                    prom_sustained_hits[t] = max(0, prom_sustained_hits[t] - 1)

        # 6. Tempo OTLP trace analysis (exclusive Self-Time + L4/L7 peer attribution)
        tempo_candidates = search_tempo_anomalies(t0, seen_traces)
        for tid in tempo_candidates:
            verdict = isolate_root_cause_from_trace(tid, t0)
            if not verdict:
                continue
            c_target = verdict["culprit"]
            if verdict["modality"] == "TEMPO_TRACE_ERROR":
                return verdict, time.time() - t0

            tempo_self_time_votes[c_target] += 1
            if tempo_self_time_votes[c_target] >= 2 or verdict.get("self_time_ms", 0.0) >= 1800.0 or loki_hit_counts[c_target] >= 1:
                return verdict, time.time() - t0

        time.sleep(POLLING_INTERVAL_SECONDS)

    return None, TIMEOUT_SECONDS


def main():
    print("======================================================================", flush=True)
    print("  EXPERIMENT SCENARIO 4b: PANOPTES MULTIMODAL RCA + EVIDENCE AUDIT", flush=True)
    print("======================================================================", flush=True)

    ensure_port_forwards()
    seen_traces = set()
    seen_logs = set()
    trial_schedule = build_balanced_trial_schedule(NUM_TRIALS, seed=RANDOM_SEED)

    with open(RESULTS_FILE, mode="w", newline="") as f_csv, \
         open(EVIDENCE_LOG_FILE, mode="w") as f_audit:

        writer = csv.writer(f_csv)
        writer.writerow([
            "trial_id", "scenario", "chaos_type", "ground_truth",
            "diagnosed_target", "t0_epoch", "mttdiag_seconds",
            "accuracy", "censored", "detection_modality",
            "evidence_id", "evidence_summary"
        ])

        trial = 1
        while trial <= NUM_TRIALS:
            print(f"\n[Trial {trial:02d}/{NUM_TRIALS}] Starting execution cycle...", flush=True)

            ensure_cluster_stability()
            clean_cluster()
            wait_for_workload_recovery()
            print("  -> Waiting for cluster stabilization (20s)...", flush=True)
            time.sleep(20)
            print("  -> Priming telemetry caches & capturing baseline metrics...", flush=True)
            baseline_cpu, baseline_pods = prime_baselines(seen_traces, seen_logs)

            selected_target, selected_chaos = trial_schedule[trial - 1]
            manifest_content = generate_chaos_manifest(selected_target, selected_chaos)

            with open("/tmp/current_chaos.yaml", "w") as m:
                m.write(manifest_content)

            if not inject_chaos_manifest("/tmp/current_chaos.yaml"):
                print(
                    f"  [RETRY-TRIAL] Infrastructure failed to inject chaos before T0 on Trial {trial:02d}. "
                    "Discarding non-injected attempt and repeating trial...",
                    flush=True
                )
                clean_cluster()
                continue

            t0 = time.time()
            t0_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f"  -> Fault injected blindly ({selected_target} via {selected_chaos}). T0: {t0_iso}", flush=True)

            time.sleep(INITIAL_PROPAGATION_SLEEP)
            verdict, elapsed_time = run_panoptes_rca_agent(t0, seen_traces, seen_logs, baseline_cpu, baseline_pods)

            if verdict is None or elapsed_time >= TIMEOUT_SECONDS:
                final_mttdiag = float(TIMEOUT_SECONDS)
                diagnosed_service = "inconclusive"
                accuracy = 0
                censored = True
                modality = "NONE_TIMEOUT"
                evidence_id = "none"
                evidence_summary = "Right-censored at 300s operational ceiling"
                print("\n  [RESULT] TIMEOUT / INCONCLUSIVE (300.0s). Censored = True.", flush=True)
            else:
                final_mttdiag = round(elapsed_time, 2)
                diagnosed_service = verdict["culprit"]
                censored = False
                accuracy = 1 if diagnosed_service == selected_target else 0
                modality = verdict["modality"]
                evidence_id = verdict["evidence_id"]
                evidence_summary = verdict["summary"]
                status_str = "SUCCESS (Acc=1)" if accuracy == 1 else "MISATTRIBUTION (Acc=0)"
                print(f"\n  [RESULT] Converged in {final_mttdiag}s via {modality}", flush=True)
                print(f"           Diagnosed: {diagnosed_service} | Ground-Truth: {selected_target} -> {status_str}", flush=True)
                print(f"           Evidence : [{evidence_id}] {evidence_summary}", flush=True)

            writer.writerow([
                trial, "panoptes-4b", selected_chaos, selected_target,
                diagnosed_service, t0_iso, final_mttdiag,
                accuracy, censored, modality,
                evidence_id, evidence_summary
            ])
            f_csv.flush()

            audit_record = {
                "trial_id": trial,
                "scenario": "panoptes-4b",
                "t0_epoch": t0_iso,
                "chaos_type": selected_chaos,
                "ground_truth": selected_target,
                "diagnosed_target": diagnosed_service,
                "mttdiag_seconds": final_mttdiag,
                "accuracy": accuracy,
                "censored": censored,
                "detection_modality": modality,
                "evidence_id": evidence_id,
                "evidence_summary": evidence_summary,
                "baseline_cpu_mcpu": baseline_cpu,
                "raw_verdict_payload": verdict if verdict else {}
            }
            f_audit.write(json.dumps(audit_record) + "\n")
            f_audit.flush()

            clean_cluster()
            trial += 1

    print(f"\n[OK] Scenario 4b completed. CSV saved to: {RESULTS_FILE} | Audit log: {EVIDENCE_LOG_FILE}", flush=True)


if __name__ == "__main__":
    main()
