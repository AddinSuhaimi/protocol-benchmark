# v1.1 correction — 26 October 2026

The freeze comparison now canonicalizes only the generated kube-api-access five-character suffix at the standard read-only service-account mount. Other mount fields and configuration checks remain enforced. Raw snapshots remain unchanged. This fixes a false configuration-drift rejection after expected payload rollouts.

Extract this package separately as psi-main-v1.1. Preserve the stopped v1 campaign and its diagnostic snapshot. Because the source hash changed, use a new campaign directory ../main-study-128-v1.1 and create a new freeze. Do not overwrite the old freeze or merge its partial runs into the new campaign. All 128 planned runs remain the same prospective schedule.

# Empirical main study — runner and analysis v1

This package implements the prospective **128-run** main study. It does not contain new experimental results. Offline tests use explicitly synthetic evidence. Pilot data remain separate calibration evidence.

Four communication protocols × 10/50 logical items × 50/100 total closed-loop users × eight complete macroblocks. Fixed eight gevent/http.client processes; approximately 30 total users/s ramp; 30 s full-concurrency warm-up and warm-up drain; 12 s idle settling; 120 s admission and drain; 0/3/6/9/12 s energy endpoints. The primary energy convention is fixed **+12 s after host-observed drain**, with no idle subtraction. The request adapter, request loop, RNG mapping, synchronization and verified-export-v3 source string are unchanged from the verified base. Collection has a new source hash; the pilot hash does not identify the modified collector.

Energy is a **model-estimated padded-batch outcome**, not physical ground truth. Exact active model weights/preprocessing remain unknown. CPU, estimated energy, performance and adverse application outcomes are reported together. CPU/energy dependence and preference substitution are prespecified; CPU is not added as an independent score dimension. Logical item counts are not measured encoded or wire bytes. No encoded-byte outcome is collected.

## Files

- `run_main.py`: plan, read-only freeze, sequential collection, status/resume.
- `collection_main.py`: validated-client collector successor; immediate summary fields preserved.
- `validate_run.py`: full client-frame/CSV reconciliation, counter/identity/source checks, explicit +12 s selection.
- `analyze_main.py`: matched analysis, block intervals, workload interactions, sensitivity, CPU dependence and figures.
- `study_common.py`: fixed design, hashes, exact metric selection and atomic bookkeeping.
- `PSI_Main_Study_Order_v0_1.json`: unchanged prespecified schedule.
- `verified-client-contract.json`: verified base and injected client-source hashes.
- `model-provenance.json`: bounded historical model evidence, explicit unresolved weights and unavailable physical reference.
- `test_study.py`, `requirements.txt`, `OFFLINE_VALIDATION.md`, `SHA256SUMS`: offline verification and file integrity.

Keep all package files together. Do not edit code after the campaign freeze. A documented change requires a new freeze/campaign; analysis detects changed source hashes.

## 1. Install and check the package

Extract the downloaded ZIP under `~/protocol-benchmark`; it contains a `psi-main-v1/` directory. It does not replace `psi_pilot.py` or old results.

```bash
cd ~/protocol-benchmark/psi-main-v1
sha256sum -c SHA256SUMS
python3 run_main.py plan
```

`plan` is offline and performs no Kubernetes operations. The manifest hash must be:

```text
770990ea730cf66f5a64d1476983e8bf722163a5494ef90e67fe937816116de9
```

Collection needs Python 3.10+ on Linux and authenticated `kubectl`; its host code uses the standard library. It reuses the existing client Pod's Python/gevent image. Do not install a different client or change the request hot path for this campaign.

Offline analysis/tests were exercised with Python 3.11 and the versions in `requirements.txt`. Prefer a separate analysis environment; collection does not require these packages:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest -v test_study
```

No tests contact Kubernetes or the telemetry endpoint. The simulated 128-run campaign is not scientific data and is deleted by the tests.

## 2. Keep the application-node exporter available

Run the port-forward inside a persistent tmux window. Reuse an already working forward rather than creating a second one on the same port.

```bash
kubectl -n kepler get pods -l app.kubernetes.io/name=kepler \
  --field-selector spec.nodeName=k3d-protocol-benchmark-agent-0 -o wide
```

Use the single returned Pod name in a tmux window:

```bash
kubectl -n kepler port-forward pod/REPLACE_WITH_RETURNED_POD_NAME 19102:9102
```

In the study window:

```bash
curl --fail --silent --show-error --max-time 10 \
  --output /dev/null http://127.0.0.1:19102/metrics
```

The freeze/runner verifies the exact application-node identity from node metric labels, the component estimator/platform-none path, the audited Kepler image digest, and the required current Gateway/Worker series. A forwarded Service that selects another node is not sufficient.

## 3. Freeze before collection

Stop any older pilot/client run. Freeze checks for existing gevent benchmark clients or Locust processes in all four load-generator Pods. This is a cooperative experiment guard, not a guarantee against unrelated activity on the shared VM.

Review `model-provenance.json`. It records the historical bounded audit and **unknown active weights**, not invented calibration. If model files or the deployment changed since the audit, document that change before collection. Known unresolved weights are allowed; a change in runtime source/image/resources is not silently accepted.

```bash
python3 run_main.py freeze \
  --output ../main-study-128-v1 \
  --provenance model-provenance.json
```

This command is read-only against Kubernetes. It writes a local freeze containing exact package/manifest/provenance hashes, actual container image IDs, container specs/resources, effective `cpu.max`, placements, node information, Kepler specification/source metadata, and a starting metrics snapshot. Unknown active weights and lack of physical calibration remain explicit. The output directory must not already have a freeze.

Expected configuration:

- Gateway and Worker on `k3d-protocol-benchmark-agent-0`; load-generator on `k3d-protocol-benchmark-agent-1`.
- Gateway/Worker CPU request `250m`, no CPU limit; all three role containers effectively uncapped (`cpu.max` begins `max `).
- Application image digest `sha256:b7279c2e887f5384b0c259862ea763c31397cc9c237d52eda921db3b7d290c72`.
- Matching protocol/role/Worker-host environment bindings; equal role resources across the four namespaces.
- Load-generator digest `sha256:aace7624daa62d895d616f5ccc861a486c742582e1445a1d52d3aae83bbaa614`, rechecked from saved completed coverage metadata.
- Kepler digest `sha256:1c822681f48d8c6f66a4613987007107322e5320f695c1cd284cef2d343bf24b`.

If a check fails, preserve the error and investigate. Do not bypass a guard just to start collection. Freeze does not prove an absence of parent cgroup quotas or physical-host contention.

## 4. Collect the main dataset

Run in a second tmux window while the port-forward stays in its own window. Collection changes Gateway `PAYLOAD_SIZE` in the four benchmark namespaces when required, waits for each rollout and an end-to-end Worker-validated `OK`, and dispatches exactly one measured run at a time. Other application configuration is verified against the freeze.

```bash
cd ~/protocol-benchmark/psi-main-v1
python3 -u run_main.py run --output ../main-study-128-v1 \
  >> ../main-study-128-v1.log 2>&1
```

This is the command that starts collection; it was not run against your server while preparing this package. No image rebuild or manifest reapplication is required by this package if the verified deployment is unchanged. Keep sufficient disk space for all raw framed exports and request CSVs: large client exports are expected. The actual campaign can take longer than the earlier 6.5–8 h arithmetic estimate because configuration checks, rollouts, exports and re-audits add overhead.

Progress from another shell:

```bash
cd ~/protocol-benchmark/psi-main-v1
python3 run_main.py status --output ../main-study-128-v1
tail -n 30 ../main-study-128-v1.log
```

`status` distinguishes accepted macroblocks from attempted/invalid blocks. `events.jsonl` records planned and actual execution; `campaign.json` atomically tracks progress. `main-outcomes.json` records the selected +12 s outcome separately from historical immediate `summary.json` fields. Preserve the entire output directory, freeze, package, and log.

## 5. Interruption and technical failures

A valid adverse application result (HTTP error, timeout, incorrect completion) remains in the accepted block. It is never automatically rerun to obtain success. Conditional successful-request latency and per-completion outcomes are retained, with failure counts. One measured failure makes the protocol/workload cell ineligible for clean comparisons across its planned repetitions.

Missing/corrupt exports, resets, missing endpoints, source/configuration drift, overlapping boundaries, quota/throttling inconsistencies or scrape errors are technical failures. The runner stops and preserves the attempt. It does not continuously rerun on error.

If a session/process is interrupted, the partially completed macroblock is invalidated when the same campaign is resumed. Completed accepted macroblocks are retained; **all 16 runs of the incomplete macroblock** are attempted again in the same prespecified order under a distinct attempt ID. At most two extra macroblock attempts are permitted across the campaign; further replacement requires stopping and documenting a repair/protocol amendment. The cap is not an error-recovery loop.

After diagnosing and repairing a transient issue, resume with the same `run` command. No favorable-value/outlier/ranking/p-value selection is implemented.

Two guards prevent cooperating main-runner instances from overlapping: a local file lock and the `psi-main-study-active` ConfigMap in the `kepler` namespace. Graceful exit removes the ConfigMap; abrupt process/host death may leave it. Before deleting a stale guard, establish that no main or pilot client remains active, inspect its details, and retain the interruption evidence:

```bash
pgrep -af '[r]un_main.py|[p]si_pilot.py'
kubectl -n kepler get configmap psi-main-study-active -o yaml
```

If confirmed stale, remove that named guard, restore the port-forward if needed, and resume:

```bash
kubectl -n kepler delete configmap psi-main-study-active
```

The runner also scans client Pods on resume; residual active clients cause a stop rather than overlapping new load. Do not delete a live guard or kill unrelated processes. If access does not permit ConfigMap creation/deletion, preserve the error for a documented guard amendment; do not launch concurrent runs.

## 6. Analyze after collection

Use the exact frozen package. Keep analysis output separate from the campaign evidence directory:

```bash
cd ~/protocol-benchmark/psi-main-v1
.venv/bin/python analyze_main.py ../main-study-128-v1 \
  --output ../main-study-analysis-v1
```

Default analysis requires eight accepted macroblocks. It independently re-audits all framed exports, merged request rows, counters and identities, then checks that the saved audit matches. This can take time and memory for large runs. It does not overwrite collection evidence. For a stopped campaign, `--partial` creates an explicitly exploratory report; it does not relabel the campaign completed.

Outputs:

- `run-level.csv/.json`: all accepted observations, including failures and undefined values.
- `analysis.json`: raw run summaries; paired JSON geometric ratios; payload/concurrency/interaction contrasts; pointwise 95% t intervals and whole-macroblock bootstrap sensitivity; fixed-endpoint sensitivity; optional weights 0.25/0.5/0.75 and CiS²-inspired products; point-estimate Pareto sets; within-workload total/per-completion and within-protocol-centered CPU–energy associations; optional BPF matched-scrape comparisons; whole-block-held-out CPU agreement with Kepler output; ranking substitution; technical attempt history.
- `tradeoffs.png/.svg`, `cpu_energy.png/.svg`: per-workload run-level figures, marked as model estimates.
- `REPORT.md`: coverage, eligibility and interpretation guide.

A missing/nonpositive JSON reference makes that block's normalized comparison undefined; no epsilon or borrowed reference is used. Zero raw observations remain in arithmetic descriptions; logarithmic summaries are undefined when zeros are present. Undefined/failing cells remain visible. Point estimates do not establish distributional dominance. The script does not declare a universal winner, production SLA compliance, independent physical energy savings, or a novel PSI index. Run-level p99 summaries are not pooled-request p99.

Main-study CPU/energy dependence is recalculated; pilot mappings are not imported. A held-out error quantifies agreement with the estimator's output, **not physical-energy accuracy**. The fixed model estimate can be reported alongside CPU/performance even when the preference rankings are redundant.

## Deployment verification boundary

The package was verified offline. No SSH access, live 120-second trial, new main data, or successful live freeze is claimed. The first server action should be the read-only plan/freeze checks above. Runtime acceptance checks apply to every main run. Any needed alteration to frozen settings is documented before a new campaign rather than silently accepted mid-study.
