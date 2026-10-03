# DSH Surge Mode

[中文](README.zh-CN.md) · **English**

[![MIT License](https://img.shields.io/badge/license-MIT-2f855a?logo=opensourceinitiative&logoColor=white)](LICENSE) [![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776ab?logo=python&logoColor=white)](pyproject.toml) [![CI](https://github.com/ZiyanZhuang/Surge/actions/workflows/ci.yml/badge.svg)](https://github.com/ZiyanZhuang/Surge/actions/workflows/ci.yml) [![DSH](https://img.shields.io/badge/DSH-Surge_Mode-6b46c1?logo=opensourceinitiative&logoColor=white)](preset-langchao.patch.yml)

We did not begin this project because we believe that adding more Agents will, by itself, bring the future closer. We began because a new kind of power is arriving: it lets one person read farther, try more possibilities, and lets a small team take difficult work apart and arrange it again. The greater the reach, the more important it becomes to keep hold of the reins—and to know when to stop.

Surge Mode is a **single-host, SQLite-backed DAG scheduler MVP** for bounded, auditable, recoverable Agent work. It is a Python execution core plus a DSH preset example. It puts budget, evidence, dependencies, recovery, and failure states in front of model calls, because we want people to use a new productive force without handing away judgment or responsibility. It is not a distributed research cluster, an LLM training project, or a completed direct DSH `llm` bridge.

[Architecture](DESIGN.zh-CN.md) · [Quick start](docs/QUICKSTART.zh-CN.md) · [WorkerAdapter contract](docs/ADAPTER-CONTRACT.zh-CN.md) · [Philosophy](PHILOSOPHY.md) · [Diagram prompt](PHILOSOPHY-DIAGRAM-PROMPT.zh-CN.md) · [Contribution hygiene](CONTRIBUTING.zh-CN.md)

## Workflow: from human questions to verifiable actions

[![Surge workflow: human goals and budget → Scout → Deepen → Verify → Synthesize → human review, supported by bounded orchestration and stop gates](<docs/assets/surge-workflow.png>)](<docs/assets/surge-workflow.png>)

*Conceptual overview; click to view the full-size image. Budgets and timeouts are illustrative, not defaults. Stop gates apply throughout execution; the implemented scope and remaining work are listed below.*

## Why “Surge”

A new tool never changes only the individual hand that holds it. It changes how people work together, how far a decision travels, and how quickly an error can spread. We borrow the image of the **rider** and the **commander**: one person may extend their reach with one Agent; a team may organize scouts, researchers, verifiers, and synthesizers—but people still set the aim, allocate scarce resources, examine the evidence, and answer for the result.

Surge Mode is not a case for an unlimited swarm. One more Agent is not automatically one more insight. The point is to make difficult work more truthful, more traceable, and more possible for the people doing it.

Read the longer essay in [PHILOSOPHY.md](PHILOSOPHY.md) or [中文哲学文](PHILOSOPHY.zh-CN.md).

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
