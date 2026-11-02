#!/usr/bin/env python3
"""
Statistical Analysis and Figure Generation for:
Chaos Engineering in Action: A Controlled Experimental Comparison of
Chaos Mesh and LitmusChaos in Cloud-Native Kubernetes Clusters
"""
import os
import numpy as np
import pandas as pd
from scipy import stats
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(REPO_ROOT, "data")
FIG_DIR = os.path.join(REPO_ROOT, "figures")
os.makedirs(FIG_DIR, exist_ok=True)

cm = pd.read_csv(os.path.join(DATA_DIR, "chaosmesh_paired_trials.csv"))
lc = pd.read_csv(os.path.join(DATA_DIR, "litmus_paired_trials.csv"))
ov = pd.read_csv(os.path.join(DATA_DIR, "chaosmesh_vs_litmus_overhead.csv"))


def vargha_delaney_a12(x, y):
    """Compute Vargha-Delaney A12 and Cliff's delta (x vs y)."""
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    nx, ny = len(x), len(y)
    more = np.sum(x[:, None] > y[None, :])
    less = np.sum(x[:, None] < y[None, :])
    ties = np.sum(x[:, None] == y[None, :])
    a12 = (more + 0.5 * ties) / (nx * ny)
    cliffs_d = (more - less) / (nx * ny)
    return a12, cliffs_d


def mcnemar_exact(acc_a, acc_b):
    """Exact binomial McNemar test on paired binary outcomes."""
    a = np.asarray(acc_a, dtype=int)
    b = np.asarray(acc_b, dtype=int)
    n11 = int(np.sum((a == 1) & (b == 1)))
    n10 = int(np.sum((a == 1) & (b == 0)))
    n01 = int(np.sum((a == 0) & (b == 1)))
    n00 = int(np.sum((a == 0) & (b == 0)))
    n_disc = n10 + n01
    if n_disc == 0:
        p_val = 1.0
    else:
        p_val = float(stats.binomtest(min(n10, n01), n_disc, 0.5, alternative="two-sided").pvalue)
    return {"n11": n11, "n10": n10, "n01": n01, "n00": n00, "p_value": p_val}


def main():
    print("=================================================================")
    print("1. RQ01: TEMPORAL ACTUATION AND TEARDOWN PRECISION (N=30)")
    print("=================================================================")
    act = lc["actuation_delay_seconds"]
    td = lc["teardown_seconds"]
    sw_act = stats.shapiro(act)
    w_act = stats.wilcoxon(act - 1.0, alternative="greater")
    print(f"Litmus Actuation Delay (s): Mean={act.mean():.2f}±{act.std():.2f}, Med={act.median():.2f} (IQR={act.quantile(0.75)-act.quantile(0.25):.2f}), Min={act.min():.2f}, Max={act.max():.2f}")
    print(f"  Shapiro-Wilk on Actuation: W={sw_act.statistic:.4f}, p={sw_act.pvalue:.4e}")
    print(f"  Wilcoxon vs 1.0s DaemonSet bound: W={w_act.statistic:.1f}, p={w_act.pvalue:.4e}")
    print(f"Litmus Teardown Latency (s): Mean={td.mean():.2f}±{td.std():.2f}, Med={td.median():.2f} (IQR={td.quantile(0.75)-td.quantile(0.25):.2f}), Min={td.min():.2f}, Max={td.max():.2f}")
    print(f"Litmus Ephemeral Pods Spawned: {int(lc['runner_pods_spawned'].sum())} pods ({int(lc['runner_pods_spawned'].mean())}/trial)")

    print("\n=================================================================")
    print("2. RQ02: STEADY-STATE RESOURCE OVERHEAD (N=30, dt=16s)")
    print("=================================================================")
    for col in ["workload_cpu_mcpu", "workload_ram_mib", "panoptes_cpu_mcpu", "panoptes_ram_mib",
                "chaos_mesh_cpu_mcpu", "chaos_mesh_ram_mib", "litmus_cpu_mcpu", "litmus_ram_mib"]:
        s = ov[col]
        print(f"  {col}: Mean={s.mean():.2f}±{s.std():.2f}, Med={s.median():.2f} (IQR={s.quantile(0.75)-s.quantile(0.25):.2f})")
    u_cpu, p_cpu = stats.mannwhitneyu(ov["chaos_mesh_cpu_mcpu"], ov["litmus_cpu_mcpu"], alternative="two-sided")
    u_ram, p_ram = stats.mannwhitneyu(ov["chaos_mesh_ram_mib"], ov["litmus_ram_mib"], alternative="two-sided")
    a12_cpu, d_cpu = vargha_delaney_a12(ov["chaos_mesh_cpu_mcpu"], ov["litmus_cpu_mcpu"])
    a12_ram, d_ram = vargha_delaney_a12(ov["chaos_mesh_ram_mib"], ov["litmus_ram_mib"])
    print(f"  CM vs LC Operator CPU: Mann-Whitney U={u_cpu:.1f}, p={p_cpu:.4e}, A12={a12_cpu:.4f}, d={d_cpu:.4f}")
    print(f"  CM vs LC Operator RAM: Mann-Whitney U={u_ram:.1f}, p={p_ram:.4e}, A12={a12_ram:.4f}, d={d_ram:.4f}")

    print("\n=================================================================")
    print("3. RQ03: DIAGNOSTIC PARITY & MTTDIAG COMPARISON (N=30 PAIRED)")
    print("=================================================================")
    acc_cm = cm["accuracy"]
    acc_lc = lc["accuracy"]
    table_2x2 = [
        [int(acc_cm.sum()), int(30 - acc_cm.sum())],
        [int(acc_lc.sum()), int(30 - acc_lc.sum())]
    ]
    odds, p_fisher = stats.fisher_exact(table_2x2)
    mcn = mcnemar_exact(acc_cm, acc_lc)
    print(f"Global Accuracy: CM={acc_cm.sum()}/30 ({acc_cm.mean()*100:.2f}%) vs LC={acc_lc.sum()}/30 ({acc_lc.mean()*100:.2f}%)")
    print(f"  Fisher's Exact Test: OR={odds:.2f}, p={p_fisher:.4f}")
    print(f"  McNemar's Paired Test: n11={mcn['n11']}, n10={mcn['n10']}, n01={mcn['n01']}, n00={mcn['n00']}, p={mcn['p_value']:.4f}")

    mtt_cm = cm["mttdiag_seconds"]
    mtt_lc_tot = lc["mttdiag_total_seconds"]
    mtt_lc_net = lc["mttdiag_net_seconds"]

    w_tot = stats.wilcoxon(mtt_cm, mtt_lc_tot, alternative="two-sided")
    a12_tot, d_tot = vargha_delaney_a12(mtt_cm, mtt_lc_tot)
    w_net = stats.wilcoxon(mtt_cm, mtt_lc_net, alternative="two-sided")
    a12_net, d_net = vargha_delaney_a12(mtt_cm, mtt_lc_net)

    print(f"  CM vs LC Total MTTDiag: Med {mtt_cm.median():.2f}s vs {mtt_lc_tot.median():.2f}s | Paired Wilcoxon W={w_tot.statistic:.1f}, p={w_tot.pvalue:.4e} | A12={a12_tot:.4f}, d={d_tot:.4f}")
    print(f"  CM vs LC Net MTTDiag:   Med {mtt_cm.median():.2f}s vs {mtt_lc_net.median():.2f}s (Mean {mtt_cm.mean():.2f}s vs {mtt_lc_net.mean():.2f}s) | Paired Wilcoxon W={w_net.statistic:.1f}, p={w_net.pvalue:.4e}")

    # =========================================================================
    # FIGURE 2: Actuation Delay & MTTDiag Comparison by Fault Stratum (No title)
    # =========================================================================
    plt.rcParams.update({"font.size": 9, "font.family": "sans-serif"})
    fig, ax = plt.subplots(figsize=(6.2, 3.2), dpi=300)

    fault_types = ["pod-kill", "cpu-stress", "network-delay"]
    labels = ["pod-kill\n(n=9)", "cpu-stress\n(n=11)", "network-delay\n(n=10)"]
    x = np.arange(len(fault_types))
    width = 0.25

    cm_meds = [cm[cm["chaos_type"] == ft]["mttdiag_seconds"].median() for ft in fault_types]
    lc_net_meds = [lc[lc["chaos_type"] == ft]["mttdiag_net_seconds"].median() for ft in fault_types]
    lc_act_meds = [lc[lc["chaos_type"] == ft]["actuation_delay_seconds"].median() for ft in fault_types]
    lc_tot_meds = [lc[lc["chaos_type"] == ft]["mttdiag_total_seconds"].median() for ft in fault_types]

    b1 = ax.bar(x - width, cm_meds, width, label="Chaos Mesh MTTDiag (DaemonSet <1s init)", color="#1f77b4", edgecolor="black", linewidth=0.7)
    b2 = ax.bar(x, lc_net_meds, width, label="LitmusChaos Net MTTDiag (Post-Actuation)", color="#2ca02c", edgecolor="black", linewidth=0.7)
    b3_net = ax.bar(x + width, lc_net_meds, width, color="#ff7f0e", edgecolor="black", linewidth=0.7, label="LitmusChaos Total MTTDiag (Net + Runner Init)")
    b3_act = ax.bar(x + width, lc_act_meds, width, bottom=lc_net_meds, color="#ffbb78", edgecolor="black", linewidth=0.7, hatch="//", label="LitmusChaos Runner Init Offset (ΔT_actuate)")

    for bar in b1:
        h = bar.get_height()
        ax.annotate(f"{h:.1f}s", xy=(bar.get_x() + bar.get_width() / 2, h),
                    xytext=(0, 2), textcoords="offset points", ha="center", va="bottom", fontsize=7.5, fontweight="bold")
    for bar in b2:
        h = bar.get_height()
        ax.annotate(f"{h:.1f}s", xy=(bar.get_x() + bar.get_width() / 2, h),
                    xytext=(0, 2), textcoords="offset points", ha="center", va="bottom", fontsize=7.5, fontweight="bold")
    for i, bar in enumerate(b3_act):
        tot_h = lc_net_meds[i] + lc_act_meds[i]
        ax.annotate(f"{lc_tot_meds[i]:.1f}s\n(+{lc_act_meds[i]:.1f}s)", xy=(bar.get_x() + bar.get_width() / 2, tot_h),
                    xytext=(0, 2), textcoords="offset points", ha="center", va="bottom", fontsize=7, fontweight="bold")

    ax.set_ylabel("Median Latency (seconds)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontweight="bold")
    ax.set_ylim(0, 82)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.legend(loc="upper left", fontsize=7.2, framealpha=0.92)
    plt.tight_layout()
    fig1_path = os.path.join(FIG_DIR, "fig_actuation_and_mttdiag.png")
    plt.savefig(fig1_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"\nSaved figure: {fig1_path}")

    # =========================================================================
    # FIGURE 3: Stratified Diagnostic Accuracy Comparison (No title)
    # =========================================================================
    fig, ax = plt.subplots(figsize=(6.0, 3.0), dpi=300)
    categories = ["Global\n(N=30)", "cpu-stress\n(n=11)", "pod-kill\n(n=9)", "network-delay\n(n=10)"]
    cm_acc = [
        cm["accuracy"].mean() * 100,
        cm[cm["chaos_type"] == "cpu-stress"]["accuracy"].mean() * 100,
        cm[cm["chaos_type"] == "pod-kill"]["accuracy"].mean() * 100,
        cm[cm["chaos_type"] == "network-delay"]["accuracy"].mean() * 100,
    ]
    lc_acc = [
        lc["accuracy"].mean() * 100,
        lc[lc["chaos_type"] == "cpu-stress"]["accuracy"].mean() * 100,
        lc[lc["chaos_type"] == "pod-kill"]["accuracy"].mean() * 100,
        lc[lc["chaos_type"] == "network-delay"]["accuracy"].mean() * 100,
    ]
    x2 = np.arange(len(categories))
    w2 = 0.34
    r1 = ax.bar(x2 - w2/2, cm_acc, w2, label="Chaos Mesh v2.6.3 (DaemonSet)", color="#1f77b4", edgecolor="black", linewidth=0.7)
    r2 = ax.bar(x2 + w2/2, lc_acc, w2, label="LitmusChaos v3.x (Ephemeral Runners)", color="#ff7f0e", edgecolor="black", linewidth=0.7)

    for bar in r1:
        h = bar.get_height()
        ax.annotate(f"{h:.1f}%", xy=(bar.get_x() + bar.get_width() / 2, h),
                    xytext=(0, 2), textcoords="offset points", ha="center", va="bottom", fontsize=7.2, fontweight="bold")
    for bar in r2:
        h = bar.get_height()
        ax.annotate(f"{h:.1f}%", xy=(bar.get_x() + bar.get_width() / 2, h),
                    xytext=(0, 2), textcoords="offset points", ha="center", va="bottom", fontsize=7.2, fontweight="bold")

    ax.set_ylabel("Root-Cause Accuracy (%)")
    ax.set_xticks(x2)
    ax.set_xticklabels(categories, fontweight="bold")
    ax.set_ylim(0, 115)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.legend(loc="lower right", fontsize=8, framealpha=0.92)
    plt.tight_layout()
    fig2_path = os.path.join(FIG_DIR, "fig_stratified_accuracy_cm_vs_lc.png")
    plt.savefig(fig2_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved figure: {fig2_path}")


if __name__ == "__main__":
    main()
