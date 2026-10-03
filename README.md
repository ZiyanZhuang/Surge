# DSH Surge Mode

Surge Mode is a **single-host, SQLite-backed DAG scheduler MVP** for bounded, auditable, recoverable Agent work. It is a Python execution core plus a DSH preset example—not a distributed cluster, an LLM training project, or a completed direct DSH `llm` bridge.

The name reflects the project's philosophy: people may ride one Agent like a productive tool, or command a bounded Agent team like a commander. The goal is not unlimited swarm size. It is to match orchestration, evidence, budget, and stopping conditions to real code, project, statistics, and research problems. See [PHILOSOPHY.md](PHILOSOPHY.md) and the [Chinese essay](PHILOSOPHY.zh-CN.md).

## Current status

The scheduler core, SQLite state, bounded per-run concurrency, four-wave gates, route selection, retries, lease recovery, budget accounting, artifact/evidence validation, offline FinQA replay, and an authorized one-case reverse-proxy smoke are implemented. Direct Python-to-DSH `llm.stream` integration, real multi-case concurrency validation, process-level hard isolation, and an independent scientific verifier remain future work.

The default test suite is offline. The real adapter smoke is opt-in and must use an authorized local Anthropic-compatible endpoint.

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

See [docs/QUICKSTART.zh-CN.md](docs/QUICKSTART.zh-CN.md) for the minimal DAG and the optional real-adapter command. See [docs/ADAPTER-CONTRACT.zh-CN.md](docs/ADAPTER-CONTRACT.zh-CN.md) for the `WorkerAdapter` boundary and fail-closed output policy. See [CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md) for clean submission and fixture-attribution rules.

## Scope and non-goals

- SQLite is suitable for a local MVP, not a distributed task queue.
- Recovery is **at-least-once**, not exactly-once.
- Python thread timeouts are cooperative; use process isolation for hard physical limits.
- Offline replay validates orchestration and evidence semantics, not general model quality or provider capacity.
- A request `max_tokens` value is not a physical upstream guarantee. The included real adapter fails closed when provider usage is missing or above the requested limit, but an upstream reverse proxy may ignore the request field.

See [README.zh-CN.md](README.zh-CN.md) for the complete feature list and [DESIGN.zh-CN.md](DESIGN.zh-CN.md) for the architecture.
