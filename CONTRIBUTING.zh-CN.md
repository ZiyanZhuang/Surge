# 贡献与提交卫生

## 本地提交前检查

在项目目录执行：

```powershell
python -B -m unittest discover -s tests -v
python -m build --sdist --wheel --outdir .release-build
python scripts/verify_release.py --dist .release-build
```

真实 provider 烟测不是默认 CI 步骤，必须明确获得授权，并且不得把 API key、原始敏感 prompt、完整日志或临时 SQLite/artifact 目录提交到 Git。

## 提交边界

- 提交源代码、文档、脱敏且可复现的 fixture 和摘要结果；
- 不提交 `.env`、凭据、token、`.sqlite3`、`smoke-runs/`、build 目录或本地虚拟环境；
- 计划文件（`plan.json`）与 `dsh-surge-run` 报告属于运行产物，提交前确认其中没有敏感 prompt；
- FinQA 派生 fixture 必须保留 `MANIFEST.json`、`README.md` 和第三方归属说明；
- 不把真实 provider 结果写成通用能力、吞吐或容量承诺；
- 改变 WorkerAdapter、预算、lease、envelope 或数据来源时，必须补测试和说明；
- 改动 `surge_cluster/http_adapter.py` 或 `run_dag.py` 时，同步更新 `docs/ADAPTER-CONTRACT.zh-CN.md` 与 `tests/test_http_adapter.py`、`tests/test_run_dag.py`；
- 真实 adapter 仍遵守 fail-closed：缺失 usage、非法 SSE、超大响应和 provider 报告超限都不能作为成功证据。

GitHub Actions 会在 Python 3.10–3.13 上运行离线回归、构建 sdist/wheel、检查归档内容并在隔离 target 中导入 wheel。
