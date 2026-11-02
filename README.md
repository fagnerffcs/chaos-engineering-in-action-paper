# Replication Package: Chaos Engineering in Action — Chaos Mesh vs. LitmusChaos in Cloud-Native Kubernetes Clusters

Este repositório contém o Pacote de Replicação (*Replication Package*) completo, auditável e aberto do experimento controlado submetido ao **20º Simpósio Brasileiro de Componentes, Arquiteturas e Reutilização de Software (SBCARS 2026 / CBSoft)**.

> **Título do Artigo:** Chaos Engineering in Action: A Controlled Experimental Comparison of Chaos Mesh and LitmusChaos in Cloud-Native Kubernetes Clusters  
> **Autores:** Fagner Silva, Jackson Raniel, Vinícius Cardoso Garcia  
> **Instituições:** Centro de Informática - UFPE | Universidade de Pernambuco (UPE)  

---

## 📁 Estrutura do Repositório

```text
.
├── data/                                          # Datasets empíricos brutos e logs de auditoria
│   ├── chaosmesh_vs_litmus_overhead.csv           # Amostragem contínua de CPU/RAM dos planos de controle (N = 30, Δt = 16s)
│   ├── chaosmesh_paired_trials.csv                # Resultados das N = 30 rodadas pareadas no Chaos Mesh (RANDOM_SEED = 42)
│   ├── chaosmesh_evidence_audit.jsonl             # Trilha de auditoria JSONL por rodada no Chaos Mesh (N = 30 registros)
│   ├── litmus_paired_trials.csv                   # Resultados das N = 30 rodadas pareadas no LitmusChaos (RANDOM_SEED = 42)
│   └── litmus_evidence_audit.jsonl                # Trilha de auditoria JSONL por rodada no LitmusChaos (N = 30 registros)
├── figures/                                       # Figuras e diagramas do artigo (sem títulos internos)
│   ├── fig-cm-vs-litmus.tex                       # Diagrama arquitetural em TikZ (DaemonSet vs. Ephemeral Job)
│   ├── fig_actuation_and_mttdiag.png              # Distribuições empíricas de ΔT_actuate e MTTDiag (RQ01 e RQ03)
│   └── fig_stratified_accuracy_cm_vs_lc.png       # Acurácia estratificada por classe de falha (RQ03)
├── scripts/                                       # Automação de provisionamento, execução experimental e gráficos
│   ├── setup_panoptes_litmus.sh                   # Provisionamento completo do cluster K3d/K3s, OTel Demo, PANOPTES, Chaos Mesh e LitmusChaos
│   ├── deploy-panoptes.yml                        # Playbook Ansible para implantação da stack PANOPTES e Chaos Mesh
│   ├── check-panoptes-health.sh                   # Script de verificação de prontidão (pre-flight check) dos microsserviços e coletores
│   ├── run-chaosmesh-paired-trials.py             # Orquestrador experimental das N = 30 rodadas no Chaos Mesh v2.6.3
│   ├── run-litmus-paired-trials.py                # Orquestrador experimental das N = 30 rodadas e coleta de overhead no LitmusChaos v3.x
│   └── analyze_chaosmesh_vs_litmus.py             # Inferência estatística não-paramétrica (RQ01–RQ04) e geração das figuras do artigo
├── sections/                                      # Código-fonte LaTeX modularizado por seções (01 a 07)
├── tables/                                        # Tabelas LaTeX modulares (arquitetura, overhead, resultados e sondas)
├── sigconf.tex                                    # Documento principal LaTeX (template ACM sigconf)
├── references.bib                                 # Base bibliográfica BibTeX
├── LICENSE                                        # Licença MIT do pacote de replicação
└── README.md                                      # Documentação detalhada do pacote de replicação
```

---

## ⚙️ Automação e Reprodução Experimental (`scripts/`)

Para atender integralmente aos padrões de **Ciência Aberta (Open Science & Empirical Standards da ACM/IEEE/SBC)**, todo o pipeline — desde o provisionamento da infraestrutura Kubernetes até a execução das rodadas pareadas de injeção de falhas e a geração dos testes estatísticos e gráficos do artigo — foi automatizado no diretório `scripts/`.

### 1. Provisionamento do Ambiente (`setup_panoptes_litmus.sh` & `deploy-panoptes.yml`)

O ambiente experimental utiliza um cluster Kubernetes (`K3d/K3s v1.33.x`) executando o benchmark distribuído **OpenTelemetry Astronomy Shop (`v3.1.0`)**, a plataforma de observabilidade multimodal **PANOPTES** (Prometheus, Loki, Jaeger e Correlator Engine) e os dois motores de Engenharia de Caos graduados/incubados pela CNCF (**Chaos Mesh v2.6.3** e **LitmusChaos v3.x**).

Para provisionar o ambiente completo do zero e realizar o *pre-pull* das imagens dos *runners* nos nós do cluster (eliminando viés de rede durante a medição de $\Delta T_{\text{actuate}}$):

```bash
# 1. Provisiona o cluster, aplica o playbook Ansible (deploy-panoptes.yml),
#    instala os CRDs/RBAC/ChaosExperiments do LitmusChaos e faz pre-pull das imagens
bash scripts/setup_panoptes_litmus.sh

# 2. (Opcional) Verifica a saúde dos pods, endpoints HTTP e coletores de telemetria
bash scripts/check-panoptes-health.sh
```

---

### 2. Geração dos Dados Empíricos (`run-chaosmesh-paired-trials.py` & `run-litmus-paired-trials.py`)

Ambos os orquestradores utilizam exatamente a mesma semente pseudoaleatória determinística (`RANDOM_SEED = 42`), garantindo o pareamento estrito rodada a rodada ($i \in \{1, \dots, 30\}$) tanto do microsserviço alvo ($s_i \in \{\text{cart}, \text{checkout}, \text{currency}, \text{frontend}, \text{product-catalog}, \text{recommendation}\}$) quanto da classe de falha ($f_i \in \{\text{pod-kill } (n=9), \text{cpu-stress } (n=11), \text{network-delay } (n=10)\}$).

```bash
# Executa as N = 30 rodadas pareadas no Chaos Mesh v2.6.3 (resident DaemonSet model)
python3 scripts/run-chaosmesh-paired-trials.py \
  --trials 30 \
  --output-csv data/chaosmesh_paired_trials.csv \
  --audit-file data/chaosmesh_evidence_audit.jsonl

# Executa a coleta de overhead (N = 30 amostras, Δt = 16s) + N = 30 rodadas pareadas no LitmusChaos v3.x
python3 scripts/run-litmus-paired-trials.py \
  --mode both \
  --trials 30 \
  --overhead-samples 30 \
  --output-dir data
```

---

### 3. Análise Estatística e Geração dos Gráficos (`analyze_chaosmesh_vs_litmus.py`)

Para reproduzir todos os testes de hipótese não-paramétricos reportados no artigo (Wilcoxon Signed-Rank, Mann-Whitney $U$, Tamanho de Efeito Vargha-Delaney $\hat{A}_{12}$, Exato de Fisher e Pareado de McNemar) e regerar os gráficos de alta resolução (`300 DPI`, sem títulos internos, conforme diretrizes de tipografia LaTeX/ACM) diretamente a partir dos CSVs em `data/`:

```bash
# Requer Python 3.8+ com numpy, scipy e matplotlib
python3 scripts/analyze_chaosmesh_vs_litmus.py
```

---

### 4. Fluxo Metodológico e Pipeline Experimental

O pipeline automatizado pelos scripts espelha com precisão o desenho experimental pareado descrito nas Seções 4 e 5 do artigo:

```text
  [ Provisionamento: setup_panoptes_litmus.sh + deploy-panoptes.yml ]
  (Cluster K3s + OTel Astronomy Shop v3.1.0 + Stack PANOPTES + Pre-pull de Imagens)
                               │
         ┌─────────────────────┴─────────────────────┐
         ▼                                           ▼
  [ Braço A: Chaos Mesh v2.6.3 ]              [ Braço B: LitmusChaos v3.x ]
  (DaemonSet Residente: chaos-daemon)         (Job Efêmero: chaos-runner + go-runner)
         │                                           │
         ├──► Coleta de Overhead (N = 30, Δt = 16s) ◄┤
         │    └──► data/chaosmesh_vs_litmus_overhead.csv (RQ02)
         │                                           │
         ▼ (RANDOM_SEED = 42, N = 30 Trials)         ▼ (RANDOM_SEED = 42, N = 30 Trials)
  [ run-chaosmesh-paired-trials.py ]          [ run-litmus-paired-trials.py ]
    ├── pod-kill (n = 9)                        ├── pod-kill (n = 9)
    ├── cpu-stress (n = 11)                     ├── cpu-stress (n = 11)
    └── network-delay (n = 10)                  └── network-delay (n = 10)
         │                                           │
         ▼                                           ▼
  [ data/chaosmesh_paired_trials.csv ]        [ data/litmus_paired_trials.csv ]
  [ data/chaosmesh_evidence_audit.jsonl ]     [ data/litmus_evidence_audit.jsonl ]
         │                                           │
         └─────────────────────┬─────────────────────┘
                               ▼
             [ scripts/analyze_chaosmesh_vs_litmus.py ]
             ├──► Testes Estatísticos (Wilcoxon, Mann-Whitney U, Â_12, Fisher, McNemar)
             ├──► figures/fig_actuation_and_mttdiag.png (RQ01 & RQ03)
             └──► figures/fig_stratified_accuracy_cm_vs_lc.png (RQ03)
```

---

## 📊 Mapeamento e Aderência Metodológica (Questões de Pesquisa RQ01–RQ04)

| Questão de Pesquisa (RQ) | Métrica / Indicador Avaliado | Resultado no Chaos Mesh (`v2.6.3`) | Resultado no LitmusChaos (`v3.x`) | Teste Estatístico / Evidência | Arquivo Correspondente |
|---|---|---|---|---|---|
| **RQ01 — Precisão Temporal e Ciclo de Vida** | Latência de Atuação ($\Delta T_{\text{actuate}}$) | **$< 1.00\text{ s}$** (*DaemonSet* residente via `nsenter`) | Mediana **$11.75\text{ s}$** ($\text{IQR}=1.15\text{ s}$, Média $12.18 \pm 2.51\text{ s}$) | Wilcoxon $W = 465.0$, $p = 9.31 \times 10^{-10}$ | `data/litmus_paired_trials.csv` |
| **RQ01 — Precisão Temporal e Ciclo de Vida** | Latência de *Teardown* ($\Delta T_{\text{teardown}}$) e *Pod Churn* | **$< 1.00\text{ s}$** / **$0$ pods efêmeros** ($3$ pods residentes) | Mediana **$2.99\text{ s}$** ($\text{IQR}=0.61\text{ s}$) / **$+2$ pods/trial** ($60$ pods em $30$ rodadas) | Auditoria de eventos Kubernetes | `data/litmus_paired_trials.csv` |
| **RQ02 — Overhead de Recursos ($N=30, \Delta t=16\text{s}$)** | Consumo de CPU e Memória RAM do Plano de Controle | **$6.47 \pm 0.68\text{ mCPU}$** (Med $6.0$) / **$118.23 \pm 0.63\text{ MiB}$** (Med $118.0$) | **$2.00 \pm 0.00\text{ mCPU}$** (Med $2.0$) / **$29.00 \pm 0.00\text{ MiB}$** (Med $29.0$) | Mann-Whitney $U = 900.0$, $p = 5.28 \times 10^{-13}$, $\hat{A}_{12} = 1.000$ | `data/chaosmesh_vs_litmus_overhead.csv` |
| **RQ03 — Paridade Diagnóstica (PANOPTES, $N=30$)** | Acurácia Global de Localização de Causa-Raiz ($Acc$) | **$25/30$ ($83.33\%$)** (`pod-kill`: $100\%$, `cpu`: $100\%$, `net`: $50\%$) | **$25/30$ ($83.33\%$)** (`pod-kill`: $100\%$, `cpu`: $81.8\%$, `net`: $70\%$) | Exato de Fisher $\text{OR}=1.00, p=1.000$; McNemar $p=1.000$ | `data/chaosmesh_paired_trials.csv` & `data/litmus_paired_trials.csv` |
| **RQ03 — Latência Diagnóstica (PANOPTES, $N=30$)** | Tempo até o Diagnóstico ($MTTDiag_{\text{total}}$ / $MTTDiag_{\text{net}}$) | Mediana **$17.27\text{ s}$** (Média $54.52 \pm 98.41\text{ s}$) | Bruto: Med **$54.52\text{ s}$** / Líquido ($MTTDiag_{\text{net}}$): Med **$42.55\text{ s}$** | Wilcoxon Pareado $W = 55.0$, $p = 7.51 \times 10^{-4}$ | `data/chaosmesh_paired_trials.csv` & `data/litmus_paired_trials.csv` |
| **RQ04 — Validação Declarativa de Hipóteses** | Sondas Nativas de Estado Estável (*Steady-State Probes*) | `StatusCheck` restrito a sondas **HTTP** externas ao CRD | **4 modalidades embutidas** (`httpProbe`, `cmdProbe`, `k8sProbe`, `promProbe`) + *Resilience Score* | Análise arquitetural e inspeção de CRDs (`ChaosResult`) | `tables/tab-probes-comparison.tex` |

---

## 📜 Licença

Os artefatos deste pacote de replicação são distribuídos sob a licença **MIT License**, permitindo sua reutilização livre, reprodutibilidade experimental e auditabilidade acadêmica.
