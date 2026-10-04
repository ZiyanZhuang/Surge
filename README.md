# DSH Surge Mode

[中文](README.zh-CN.md) · **English**

[![MIT License](https://img.shields.io/badge/license-MIT-2f855a?logo=opensourceinitiative&logoColor=white)](LICENSE) [![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776ab?logo=python&logoColor=white)](pyproject.toml) [![CI](https://github.com/ZiyanZhuang/Surge/actions/workflows/ci.yml/badge.svg)](https://github.com/ZiyanZhuang/Surge/actions/workflows/ci.yml) [![DSH](https://img.shields.io/badge/DSH-Surge_Mode-6b46c1?logo=opensourceinitiative&logoColor=white)](preset-langchao.patch.yml)

We did not begin this project because we believe that adding more Agents will, by itself, bring the future closer. We began because a new kind of power is arriving: it lets one person read farther, try more possibilities, and lets a small team take difficult work apart and arrange it again. The greater the reach, the more important it becomes to keep hold of the reins—and to know when to stop.

Surge Mode is a **single-host, SQLite-backed DAG scheduler MVP** for bounded, auditable, recoverable Agent work. It is a Python execution core plus a DSH preset example. It puts budget, evidence, dependencies, recovery, and failure states in front of model calls, because we want people to use a new productive force without handing away judgment or responsibility. It is not a distributed research cluster, an LLM training project, or a completed direct DSH `llm` bridge.

[Architecture](DESIGN.zh-CN.md) · [Quick start](docs/QUICKSTART.zh-CN.md) · [WorkerAdapter contract](docs/ADAPTER-CONTRACT.zh-CN.md) · [Philosophy](PHILOSOPHY.md) · [Diagram prompt](PHILOSOPHY-DIAGRAM-PROMPT.zh-CN.md) · [Contribution hygiene](CONTRIBUTING.zh-CN.md)

## Names used by this project

The same project appears under several identifiers across tools. `Surge` and `浪潮模式` are the English and Chinese names of one project; `surge_cluster` is its Python import name.

| Where | Name |
|---|---|
| GitHub repository | `Surge` |
| Distribution | `dsh-surge-mode` |
| DSH preset | `浪潮模式` / id `langchao` |
| Python package | `surge_cluster` |
| CLI | `dsh-surge-*` (`dsh-surge-run`, `dsh-surge-benchmark`, …) |
| Repository directory | `浪潮模式` |

## Workflow: from human questions to verifiable actions

[![Surge workflow: human goals and budget → Scout → Deepen → Verify → Synthesize → human review, supported by bounded orchestration and stop gates](<docs/assets/surge-workflow.png>)](<docs/assets/surge-workflow.png>)

*Conceptual overview; click to view the full-size image. Budgets and timeouts are illustrative, not defaults. Stop gates apply throughout execution; the implemented scope and remaining work are listed below.*

## Why “Surge”

A new tool never changes only the individual hand that holds it. It changes how people work together, how far a decision travels, and how quickly an error can spread. We borrow the image of the **rider** and the **commander**: one person may extend their reach with one Agent; a team may organize scouts, researchers, verifiers, and synthesizers—but people still set the aim, allocate scarce resources, examine the evidence, and answer for the result.

Surge Mode is not a case for an unlimited swarm. One more Agent is not automatically one more insight. The point is to make difficult work more truthful, more traceable, and more possible for the people doing it.

Read the longer essay in [PHILOSOPHY.md](PHILOSOPHY.md) or [中文哲学文](PHILOSOPHY.zh-CN.md).

## Current status

The scheduler core, SQLite state, bounded per-run concurrency, four-wave gates, route selection, retries, lease recovery, budget accounting, artifact/evidence validation, offline FinQA replay, the first-class `surge_cluster.HttpWorkerAdapter`, the `dsh-surge-run` CLI, process-level isolation (`IsolatedAdapter`), the independent `EvidenceVerifier`, and the read-only audit surface are implemented. Direct Python-to-DSH `llm.stream` integration, a live multi-case concurrency run, model-side verification, and an independent scientific verifier remain future work.

The project targets research workflows, but the only real dataset exercised so far is the FinQA financial question-answering fixture; scientific-domain tasks are unverified. The default test suite is offline; real calls are opt-in, require an explicitly authorized endpoint, and are not part of the default regression run.

## Running a real DAG in one command

The repository ships a runnable four-wave example at [examples/plan.example.json](examples/plan.example.json). A task plan is a JSON file:

```json
{
  "schema_version": 1,
  "run": {"id": "demo", "budget_cost": 5.0, "max_workers": 2, "deadline_seconds": 600},
  "nodes": [
    {"id": "scout", "prompt": "collect evidence", "wave": 0, "max_tokens": 4000, "timeout_seconds": 120},
    {"id": "synth", "prompt": "summarize verified evidence only", "wave": 1, "depends_on": ["scout"], "max_tokens": 4000}
  ]
}
```

Validate the plan offline first, with no network call:

```powershell
python run_dag.py --plan plan.json --output report.json --dry-run
```

Then, only after you have authorized a real call:

```powershell
python run_dag.py --plan plan.json --output report.json `
  --endpoint http://127.0.0.1:17800/v1/messages --model gpt-6.1-sol
```

Exit codes: `0` all nodes succeeded, `1` a node failed or a downstream node was blocked, `2` an invalid plan or argument. The report carries a UTC timestamp, an environment snapshot, the plan SHA-256, per-node status, a budget snapshot, event counts, per-call metadata, and a provenance section; prompt text is never included.

Optional switches: `--isolate` runs every model call in its own worker process (hard timeout via terminate/kill, strict physical concurrency); `--verify annotate|fail|off` adds independent evidence verification, where `fail` makes a failed verification fail the node and block downstream; `--no-provenance` omits the provenance section.

`node.max_tokens` is the per-call budget envelope (input plus output); the actual request ceiling is `min(--max-output-tokens, node.max_tokens)`, and the adapter fails closed when provider-reported output exceeds that ceiling.

## Independent verification and provenance

`EvidenceVerifier` checks each claim for missing evidence, dangling references, self-reference (a claim whose only evidence comes from the node that produced it), and answer/evidence agreement. `VerifyingAdapter` attaches the result under `envelope["verification"]` and appends issues to `warnings`; it never rewrites the answer, the claims, or any artifact. With `policy="fail"` a failed verification fails the node instead.

`ModelVerifier` runs a second model call as the verifier and requires a strict `{"pass", "score", "issues"}` verdict. Unparseable verifier output counts as **not verified**, never as a pass, and the verifier's tokens are folded back into the node usage so budget settlement stays truthful.

Envelopes are persisted per attempt (successful and validation-failed alike), so claim-level provenance can be rebuilt after the run; oversized envelopes are stored as a claim-preserving projection with digests instead of being silently truncated.

## DSH Host `llm.stream` bridge

`dsh-bridge/dsh-llm-bridge.mjs` is a Host plugin that maps `ctx.llm.stream(GenerateOptions)` onto Anthropic Messages SSE, which lets the Python side reuse the same verified `HttpWorkerAdapter` instead of growing a second transport. It binds loopback only, requires a token file, caps concurrency with HTTP 429, never fabricates usage the upstream did not report, and emits an `error` event without `message_stop` when the upstream fails or aborts.

`node dsh-bridge/selftest.mjs` covers the protocol with a stub context (34 checks, also run in CI); `dsh-bridge/golden-sse.txt` is replayed by `tests/test_bridge_contract.py` so both sides stay pinned to one wire format. Activating the plugin requires a profile patch and a Host restart, which is left to the operator.

## Bounded concurrency against a real endpoint (Gate C)

`benchmark_gate_c.py` submits three FinQA cases as one run with `max_workers=2`, so the third wave-0 node has to wait for a slot. Each case has a real `solve` node and a deterministic `check` node that re-reads the persisted model response and compares it against the frozen oracle. The report includes observed peak concurrency, the budget invariant, heartbeat event counts, per-call metadata, the verification summary, and provenance.

It refuses to run when the estimated input tokens plus the output ceiling would not fit the declared `node.max_tokens`, so a run cannot silently die mid-way on the hard budget. Gate C has been executed: 2026-10-04T03:04:17Z, three cases, `max_workers=2`, observed peak concurrency 2, 6/6 nodes succeeded, budget invariant intact, settled cost 0.00935. See [gate-c.json](smoke-results/gate-c.json) for the raw record.

## Real-provider capacity curve (Gate D)

`benchmark_gate_d.py` fires N simultaneous real calls per level with `max_attempts=1`, using a deliberately trivial task so the measurement reflects transport and provider pressure rather than task difficulty. Executed 2026-10-04T03:25:30Z:

| Concurrency | Valid | Valid rate | Peak | Failure kinds | Wall |
|---:|---:|---:|---:|---|---:|
| 16 | 13/16 | 81.25% | 16 | `http` × 3 | 23.1 s |
| 32 | 27/32 | 84.38% | 32 | `http` × 5 | 23.2 s |
| 64 | 0/64 | 0% | 64 | `http` × 64 | 5.3 s |

Every failure at 64 was an upstream `429` wrapped as `502`, and the whole burst was refused in 5.3 seconds at zero cost, so this endpoint's burst ceiling sits between 32 and 64. That also explains the older staggered result (16 batches of 4 returning 61/64): pacing keeps the instantaneous concurrency under the ceiling. This is a burst measurement, not a quota, SLA, or sustained-throughput claim. Raw record: [gate-d.json](smoke-results/gate-d.json).

## Preset versus executor

The DSH `浪潮模式` preset registers a model-visible persona and tool set; it does not run the Python scheduler. Real execution is the job of `dsh-surge-run`: the model is expected to write a JSON plan, validate it with `--dry-run`, and only reach a live endpoint once the user authorizes it. Wave order, budget, evidence references, and failure states are then checked by code, while the persona carries the decomposition and working discipline. Without that CLI, the four waves and gates in the preset are prompt conventions only.

## Quick start

From the workspace root on Windows:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\python -m pip install -e ".\浪潮模式[dev]"
Set-Location .\浪潮模式
python -B -m unittest discover -s tests -v
python -m build --sdist --wheel --outdir dist
python scripts/verify_release.py --dist dist
```

See [docs/QUICKSTART.zh-CN.md](docs/QUICKSTART.zh-CN.md) for the minimal DAG and the optional real-adapter command. See [docs/ADAPTER-CONTRACT.zh-CN.md](docs/ADAPTER-CONTRACT.zh-CN.md) for the `WorkerAdapter` boundary, the decorator composition order and the audit surface. See [CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md) for clean submission and fixture-attribution rules.

## Capabilities

- Persistent Run, Node, Attempt, Artifact, budget ledger and event state in SQLite
- Dependency validation, cycle detection, a priority ready queue, and bounded per-run `max_workers` concurrency
- Per-stage `max_in_flight`/`dispatch_batch` incremental dispatch instead of unconditional fan-out
- Demand computed from `incremental_value`, `urgency` and live completion progress; workers report monotonic progress through `context["report_progress"]`
- Four discrete waves: Scout → Deepen → Verify → Synthesize, with a gate that keeps the next wave from starting below the success threshold
- `RouteProfile` selection by task class, stage quality floor, concurrency capacity, budget and progress
- Low-marginal nodes recorded as `deferred` once the incremental target is met
- Bounded retries, exponential backoff, cooperative timeouts, and downstream `blocked` propagation
- Lease heartbeat and stale-lease recovery, with explicit **at-least-once** semantics
- `strict`/`lenient` envelope policies, strict JSON validation, and evidence-reference checking
- Content-addressed artifacts with atomic commits and SHA-256 digests
- Budget reservation before the call and settlement after it, with a hard ceiling
- `RunResult.blocked` aggregates `blocked` and `cancelled`; read `snapshot()` to tell them apart

Node timeouts emit a cooperative stop signal and hold the old slot until the thread ends, so a retry cannot overlap the previous call or exceed the logical concurrency limit. For hard timeouts and strict physical concurrency, use `IsolatedAdapter` or `dsh-surge-run --isolate`, which run each call in its own process.

## Minimal usage

```python
from surge_cluster import DAGScheduler, NodeSpec, RunSpec, LocalEchoAdapter

run = RunSpec(id="demo", budget_cost=10.0, max_nodes=20)
nodes = [
    NodeSpec(id="scout", wave=0, prompt="collect evidence", max_attempts=2),
    NodeSpec(id="synth", wave=3, prompt="summarize verified evidence only", depends_on=["scout"]),
]
with DAGScheduler("demo.sqlite3", max_workers=4) as scheduler:
    scheduler.submit(run, nodes)
    result = scheduler.execute(LocalEchoAdapter())
    print(result.status)
```

## DSH preset registration and integration boundary

[preset-langchao.patch.yml](preset-langchao.patch.yml) is an overlay example for a user profile; it does not modify the current DSH profile automatically. Merge its patch into the profile's `cordis.patch.yml` (back the file up first), refresh the DSH page, restart the Host if needed, and pick **浪潮模式** in the mode selector. It keeps standard/ptc/minimal/cordis and adds `preset-langchao`. To ship it in a distribution, move the preset into `dsh-web-app/presets/` and update the package files and bundle patch.

`WorkerAdapter` is the stable seam. The scheduler never guesses at or calls a DSH Service directly. `surge_cluster.HttpWorkerAdapter` is a first-class library adapter aimed at an authorized local reverse proxy or a compatible Messages endpoint; a direct Python-to-DSH `llm.stream` business-Service bridge still has to be implemented and reviewed in a Host plugin. Profile patches only register model-visible modes and tool sets; multi-process and multi-host execution need a separate runtime design.

## Scope and non-goals

- Single host by design; SQLite is suitable for a local MVP, not a distributed task queue.
- Recovery is **at-least-once**, not exactly-once.
- Python thread timeouts are cooperative by default; use `--isolate` (or `IsolatedAdapter`) for hard timeouts, which terminate the worker process.
- Offline replay validates orchestration and evidence semantics, not general model quality or provider capacity.
- A request `max_tokens` value is not a physical upstream guarantee. The included real adapter fails closed when provider usage is missing or above the requested limit, but an upstream reverse proxy may ignore the request field.
- The local scheduler benchmark measures a synthetic `sleep`-shaped workload. Its throughput number is a machine-dependent observation, not a capability metric.

See [README.zh-CN.md](README.zh-CN.md) for the complete feature list and [DESIGN.zh-CN.md](DESIGN.zh-CN.md) for the architecture.
