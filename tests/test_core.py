import shutil
import sqlite3
import sys
import threading
import time
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from surge_cluster import ArtifactStore, DAGScheduler, NodeSpec, RouteProfile, RunSpec, StagePolicy, WorkerResult


class CountingAdapter:
    def __init__(self, *, fail_once=None, delay=0.0, invalid=False, with_claim=False):
        self.fail_once = set(fail_once or ())
        self.delay = delay
        self.invalid = invalid
        self.with_claim = with_claim
        self.calls = {}
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def run(self, node, context):
        with self.lock:
            self.calls[node.id] = self.calls.get(node.id, 0) + 1
            call = self.calls[node.id]
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            if node.id in self.fail_once and call == 1:
                raise RuntimeError("transient")
            if self.invalid:
                return WorkerResult({"answer": "bad", "claims": [], "citations": [], "confidence": 2, "warnings": [], "usage": {}})
            claims = []
            if self.with_claim and context.get("artifacts", {}).get("notes"):
                claims = [{"id": "c1", "text": "derived from a predecessor artifact", "evidence_refs": [context["artifacts"]["notes"]]}]
            return WorkerResult(
                {
                    "answer": node.prompt,
                    "claims": claims,
                    "citations": [],
                    "confidence": 0.8,
                    "warnings": [],
                    "usage": {"total_tokens": 10, "cost": 0.001},
                },
                {"notes": f"artifact for {node.id}"},
            )
        finally:
            with self.lock:
                self.active -= 1


class RoutingAdapter(CountingAdapter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.routes = {}
        self.stages = {}

    def run(self, node, context):
        with self.lock:
            self.routes[node.id] = context.get("route")
            self.stages[node.id] = context.get("stage")
        return super().run(node, context)


class ProgressAdapter(CountingAdapter):
    def run(self, node, context):
        self.progress_results = [context["report_progress"](0.25, "started"), context["report_progress"](0.1, "regression"), context["report_progress"](0.75, "evidence ready")]
        return super().run(node, context)


class TimeoutRetryAdapter:
    def __init__(self):
        self.calls = 0
        self.first_started = threading.Event()

    def run(self, node, context):
        self.calls += 1
        if self.calls == 1:
            self.first_started.set()
            time.sleep(0.12)  # 故意不合作，验证超时后仍保留槽位且不与重试重叠。
        return WorkerResult({
            "answer": "ok",
            "claims": [],
            "citations": [],
            "confidence": 0.8,
            "warnings": [],
            "usage": {"total_tokens": 1, "cost": 0.0001},
        })


class OverrunAdapter:
    def run(self, node, context):
        return WorkerResult(
            {
                "answer": "over budget",
                "claims": [],
                "citations": [],
                "confidence": 0.8,
                "warnings": [],
                "usage": {"total_tokens": 10, "cost": 5.0},
            },
            {"notes": "should be removed after validation failure"},
        )


class InvalidArtifactAdapter:
    def run(self, node, context):
        return WorkerResult(
            {"answer": "invalid", "claims": [], "citations": [], "confidence": 2, "warnings": [], "usage": {"cost": 0.001}},
            {"notes": "failed attempt artifact"},
        )


class HeartbeatAdapter(CountingAdapter):
    def run(self, node, context):
        self.heartbeat_accepted = context["heartbeat"](0.2)
        time.sleep(0.07)
        return super().run(node, context)


class LeaseAdapter:
    def __init__(self):
        self.started = threading.Event()

    def run(self, node, context):
        self.started.set()
        time.sleep(0.08)
        return WorkerResult({
            "answer": "ok",
            "claims": [],
            "citations": [],
            "confidence": 0.8,
            "warnings": [],
            "usage": {"total_tokens": 1, "cost": 0.0001},
        })


class CancellationAdapter:
    def __init__(self):
        self.started = threading.Event()
        self.stopped = threading.Event()

    def run(self, node, context):
        self.started.set()
        while not context["should_stop"]():
            time.sleep(0.005)
        self.stopped.set()
        return WorkerResult({
            "answer": "late",
            "claims": [],
            "citations": [],
            "confidence": 0.8,
            "warnings": [],
            "usage": {"total_tokens": 1, "cost": 0.0001},
        })


class CoreTests(unittest.TestCase):
    def _run_dir(self):
        directory = Path(__file__).resolve().parents[1] / f".test-run-{uuid.uuid4().hex}"
        directory.mkdir()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        return directory

    def scheduler(self, directory):
        return DAGScheduler(Path(directory) / "run.sqlite3")

    def test_dag_executes_with_bounded_concurrency_and_artifacts(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            nodes = [NodeSpec(f"n{i}", f"work {i}", wave=0) for i in range(6)]
            nodes.append(NodeSpec("synth", "synth", depends_on=tuple(f"n{i}" for i in range(6)), wave=1))
            scheduler.submit(RunSpec("r1", budget_cost=10, max_workers=2), nodes)
            adapter = CountingAdapter(delay=0.015, with_claim=True)
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(len(result.succeeded), 7)
            self.assertLessEqual(adapter.max_active, 2)
            stored = scheduler._conn.execute(
                "SELECT path FROM artifacts WHERE run_id='r1' LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(stored)
            artifact_root = Path(stored["path"]).parent
            self.assertTrue(artifact_root.exists())
            self.assertEqual(artifact_root.parent.name, "run-artifacts")
            self.assertNotEqual(artifact_root.name, "r1")

    def test_high_concurrency_fanout_stays_bounded(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            nodes = [NodeSpec(f"fanout-{i}", "parallel work", wave=0) for i in range(32)]
            scheduler.submit(RunSpec("fanout", budget_cost=10, max_workers=8), nodes)
            adapter = CountingAdapter(delay=0.005)
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(len(result.succeeded), 32)
            self.assertLessEqual(adapter.max_active, 8)

    def test_cycle_and_missing_dependency_are_rejected(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            with self.assertRaisesRegex(ValueError, "cycle"):
                scheduler.submit(RunSpec("cycle"), [NodeSpec("a", "a", depends_on=("b",)), NodeSpec("b", "b", depends_on=("a",))])
            with self.assertRaisesRegex(ValueError, "missing"):
                scheduler.submit(RunSpec("missing"), [NodeSpec("a", "a", depends_on=("nope",))])

    def test_transient_failure_is_retried_and_final_failure_blocks_downstream(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(
                RunSpec("r2", budget_cost=10),
                [NodeSpec("flaky", "flaky", max_attempts=2), NodeSpec("after", "after", depends_on=("flaky",))],
            )
            adapter = CountingAdapter(fail_once={"flaky"})
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(adapter.calls["flaky"], 2)
            self.assertIn("after", result.succeeded)

        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(
                RunSpec("r3", budget_cost=10),
                [NodeSpec("bad", "bad", max_attempts=1), NodeSpec("after", "after", depends_on=("bad",))],
            )
            adapter = CountingAdapter(fail_once={"bad"})
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.failed, ("bad",))
            self.assertEqual(result.blocked, ("after",))

    def test_wave_gate_and_hard_budget(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(
                RunSpec("r4", budget_cost=10, wave_success_threshold=1.0),
                [NodeSpec("bad", "bad", wave=0, max_attempts=1), NodeSpec("next", "next", wave=1)],
            )
            result = scheduler.execute(CountingAdapter(fail_once={"bad"}))
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.blocked, ("next",))

        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("r5", budget_cost=0.001, cost_per_1k_tokens=0.01), [NodeSpec("too-big", "too-big", max_tokens=1000)])
            result = scheduler.execute(CountingAdapter())
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.blocked, ("too-big",))

    def test_worker_cost_overrun_is_capped_and_audited(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(
                RunSpec("overrun", budget_cost=1.0, cost_per_1k_tokens=0.001),
                [NodeSpec("expensive", "expensive", max_tokens=10, max_attempts=1)],
            )
            result = scheduler.execute(OverrunAdapter())
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.failed, ("expensive",))
            row = scheduler._conn.execute("SELECT spent_cost FROM runs WHERE id=?", ("overrun",)).fetchone()
            self.assertEqual(row["spent_cost"], 1.0)
            overrun = scheduler._conn.execute(
                "SELECT amount FROM budget_ledger WHERE run_id=? AND operation='overrun'", ("overrun",)
            ).fetchone()
            self.assertAlmostEqual(overrun["amount"], 4.0)

    def test_budget_charge_preserves_other_reservations(self):
        finished_a = threading.Event()
        both_started = threading.Barrier(2)

        class ConcurrentOverrunAdapter:
            def run(self, node, context):
                both_started.wait(1.0)
                if node.id == "a":
                    finished_a.set()
                    cost = 10.0
                else:
                    finished_a.wait(1.0)
                    time.sleep(0.08)
                    cost = 0.0
                return WorkerResult(
                    {
                        "answer": "budget test",
                        "claims": [],
                        "citations": [],
                        "confidence": 0.8,
                        "warnings": [],
                        "usage": {"total_tokens": 1, "cost": cost},
                    }
                )

        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(
                RunSpec("reservation-cap", budget_cost=10.0, cost_per_1k_tokens=1.0, max_workers=2),
                [NodeSpec("a", "a", max_tokens=5000, max_attempts=1), NodeSpec("b", "b", max_tokens=5000, max_attempts=1)],
            )
            result = scheduler.execute(ConcurrentOverrunAdapter())
            self.assertEqual(result.status, "failed")
            charge = scheduler._conn.execute(
                "SELECT amount FROM budget_ledger WHERE run_id='reservation-cap' AND node_id='a' AND operation='charge'"
            ).fetchone()
            self.assertIsNotNone(charge)
            self.assertAlmostEqual(charge["amount"], 5.0)
            self.assertLessEqual(result.spent_cost, 10.0)

    def test_failed_attempt_artifact_binding_is_removed(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("artifact-fail", budget_cost=10), [NodeSpec("bad", "bad", max_attempts=1)])
            result = scheduler.execute(InvalidArtifactAdapter())
            self.assertEqual(result.status, "failed")
            count = scheduler._conn.execute("SELECT COUNT(*) AS count FROM artifacts WHERE run_id=?", ("artifact-fail",)).fetchone()["count"]
            self.assertEqual(count, 0)

    def test_heartbeat_and_stale_lease_recovery(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("r7", budget_cost=10), [NodeSpec("long", "long", max_attempts=2)])
            now = time.time()
            with scheduler._lock, scheduler._conn:
                scheduler._conn.execute("UPDATE runs SET owner_id=? WHERE id=?", (scheduler._owner_id, "r7"))
                scheduler._conn.execute("UPDATE nodes SET status='running',attempt_count=1,current_attempt_id=? WHERE run_id=? AND id=?", ("a7", "r7", "long"))
                scheduler._conn.execute("INSERT INTO attempts(id,run_id,node_id,owner_id,number,status,lease_until,started_at) VALUES(?,?,?,?,?,?,?,?)", ("a7", "r7", "long", scheduler._owner_id, 1, "running", now + 1, now))
                self.assertTrue(scheduler.heartbeat("r7", "long", "a7", 30))
                scheduler._conn.execute("UPDATE attempts SET lease_until=? WHERE id=?", (now - 1, "a7"))
            self.assertEqual(scheduler.recover_stale("r7", now=now), ("long",))
            self.assertEqual(scheduler.snapshot("r7")[0]["status"], "pending")

    def test_invalid_evidence_envelope_is_retried_then_failed(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("r6", budget_cost=10), [NodeSpec("claim", "claim", max_attempts=1)])
            result = scheduler.execute(CountingAdapter(invalid=True))
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.failed, ("claim",))

    def test_very_old_schema_migrates_all_runtime_tables(self):
        directory = self._run_dir()
        db_path = Path(directory) / "very-old.sqlite3"
        connection = sqlite3.connect(db_path)
        connection.executescript(
            """
            CREATE TABLE runs (id TEXT PRIMARY KEY);
            CREATE TABLE nodes (run_id TEXT, id TEXT, PRIMARY KEY (run_id, id));
            CREATE TABLE attempts (id TEXT PRIMARY KEY);
            CREATE TABLE artifacts (ref TEXT PRIMARY KEY, path TEXT, sha256 TEXT);
            CREATE TABLE budget_ledger (id INTEGER PRIMARY KEY);
            CREATE TABLE events (id INTEGER PRIMARY KEY);
            """
        )
        connection.close()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("migrated", budget_cost=10), [NodeSpec("n", "n")])
            result = scheduler.execute(CountingAdapter())
            self.assertEqual(result.status, "succeeded")
            self.assertTrue(scheduler._conn.execute("SELECT 1 FROM events WHERE run_id='migrated'").fetchone())

    def test_previous_schema_gets_additive_demand_columns(self):
        directory = self._run_dir()
        db_path = Path(directory) / "legacy.sqlite3"
        connection = sqlite3.connect(db_path)
        connection.executescript(
            """
            CREATE TABLE runs (
                id TEXT PRIMARY KEY, status TEXT NOT NULL, budget_cost REAL NOT NULL,
                reserved_cost REAL NOT NULL DEFAULT 0, spent_cost REAL NOT NULL DEFAULT 0,
                max_nodes INTEGER NOT NULL, max_workers INTEGER NOT NULL,
                wave_success_threshold REAL NOT NULL, cost_per_1k_tokens REAL NOT NULL,
                deadline_at REAL NOT NULL, cancel_requested INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL
            );
            CREATE TABLE nodes (
                run_id TEXT NOT NULL, id TEXT NOT NULL, prompt TEXT NOT NULL,
                depends_on TEXT NOT NULL, wave INTEGER NOT NULL, priority INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL, timeout_seconds REAL NOT NULL, max_tokens INTEGER NOT NULL,
                validation_policy TEXT NOT NULL, status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0, next_ready_at REAL NOT NULL DEFAULT 0,
                error TEXT, PRIMARY KEY (run_id, id)
            );
            """
        )
        connection.close()
        with self.scheduler(directory) as scheduler:
            run_columns = {row[1] for row in scheduler._conn.execute("PRAGMA table_info(runs)")}
            node_columns = {row[1] for row in scheduler._conn.execute("PRAGMA table_info(nodes)")}
            self.assertTrue({"stage_policies", "route_profiles"}.issubset(run_columns))
            self.assertTrue({"stage", "incremental_value", "urgency", "route_class", "progress"}.issubset(node_columns))

    def test_stage_demand_caps_inflight_and_routes_by_task_class(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            run = RunSpec(
                "demand-route",
                budget_cost=10,
                max_workers=3,
                stage_policies=(
                    StagePolicy("scout", max_in_flight=2, dispatch_batch=2, quality_floor=0.2),
                    StagePolicy("verify", max_in_flight=1, dispatch_batch=1, quality_floor=0.8),
                ),
                route_profiles=(
                    RouteProfile("scout-fast", "openai-codex", "gpt-5.6-sol", quality=0.4, latency=0.4, cost_per_1k_tokens=0.001, max_concurrency=2, route_classes=("scout",)),
                    RouteProfile("verify-deep", "openai-codex", "gpt-6.1-sol", reasoning_effort="high", quality=0.95, latency=2.0, cost_per_1k_tokens=0.01, max_concurrency=1, route_classes=("verify",)),
                ),
            )
            scout_nodes = [
                NodeSpec(f"scout-{i}", "scout", stage="scout", route_class="scout", wave=0, incremental_value=0.5)
                for i in range(3)
            ]
            nodes = scout_nodes + [
                NodeSpec("verify", "verify", stage="verify", route_class="verify", wave=1, depends_on=tuple(node.id for node in scout_nodes), incremental_value=1.0, urgency=1.0)
            ]
            scheduler.submit(run, nodes)
            adapter = RoutingAdapter(delay=0.01)
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(adapter.routes["scout-0"]["profile_id"], "scout-fast")
            self.assertEqual(adapter.routes["verify"]["profile_id"], "verify-deep")
            self.assertEqual(adapter.stages["verify"], "verify")
            event_rows = scheduler._conn.execute("SELECT event FROM events WHERE run_id=?", ("demand-route",)).fetchall()
            self.assertTrue(any(row["event"] == "task.routed" for row in event_rows))

    def test_incompatible_route_is_blocked_without_deadlock(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(
                RunSpec(
                    "route-block",
                    budget_cost=10,
                    route_profiles=(RouteProfile("verify-only", "openai-codex", "gpt-6.1-sol", route_classes=("verify",)),),
                ),
                [NodeSpec("scout", "scout", route_class="scout", max_attempts=1)],
            )
            result = scheduler.execute(CountingAdapter())
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.blocked, ("scout",))

    def test_incremental_target_defers_excess_agents(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(
                RunSpec(
                    "incremental",
                    budget_cost=10,
                    max_workers=4,
                    stage_policies=(StagePolicy("scout", max_in_flight=2, dispatch_batch=2, target_progress=0.5),),
                ),
                [NodeSpec(f"candidate-{i}", "candidate", stage="scout", wave=0, incremental_value=1.0) for i in range(4)],
            )
            adapter = CountingAdapter(delay=0.01)
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(len(result.succeeded), 2)
            self.assertEqual(len(result.deferred), 2)
            self.assertEqual(sum(adapter.calls.values()), 2)
            self.assertTrue(all(item["status"] in {"succeeded", "deferred"} for item in scheduler.snapshot("incremental")))

    def test_incremental_target_accounts_for_same_loop_dispatches(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(
                RunSpec(
                    "incremental-cap",
                    budget_cost=10,
                    max_workers=4,
                    stage_policies=(StagePolicy("scout", max_in_flight=4, dispatch_batch=4, target_progress=0.25),),
                ),
                [NodeSpec(f"candidate-{i}", "candidate", stage="scout", wave=0, incremental_value=1.0) for i in range(4)],
            )
            adapter = CountingAdapter(delay=0.01)
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(len(result.succeeded), 1)
            self.assertEqual(len(result.deferred), 3)
            self.assertEqual(sum(adapter.calls.values()), 1)

    def test_worker_progress_is_monotonic_and_audited(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("progress", budget_cost=10), [NodeSpec("progress-node", "progress")])
            adapter = ProgressAdapter()
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(adapter.progress_results, [True, False, True])
            snapshot = scheduler.snapshot("progress")
            self.assertEqual(snapshot[0]["progress"], 1.0)
            progress_events = scheduler._conn.execute("SELECT detail FROM events WHERE run_id=? AND event='task.progress'", ("progress",)).fetchall()
            self.assertEqual(len(progress_events), 2)

    def test_timeout_keeps_old_slot_until_thread_stops_then_retries(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("timeout", budget_cost=10, max_workers=1), [NodeSpec("slow", "slow", max_attempts=2, timeout_seconds=0.02)])
            adapter = TimeoutRetryAdapter()
            started = time.perf_counter()
            result = scheduler.execute(adapter)
            elapsed = time.perf_counter() - started
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(adapter.calls, 2)
            self.assertLess(elapsed, 0.50)
            self.assertEqual(scheduler.snapshot("timeout")[0]["attempt_count"], 2)

    def test_deadline_cas_failure_settles_attempt_and_reservation(self):
        class SlowEnvelope(dict):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._slept = False

            def get(self, key, default=None):
                if not self._slept:
                    self._slept = True
                    time.sleep(0.08)
                return super().get(key, default)

        class SlowValidationAdapter:
            def run(self, node, context):
                return WorkerResult(
                    SlowEnvelope(
                        answer="slow validation",
                        claims=[],
                        citations=[],
                        confidence=0.5,
                        warnings=[],
                        usage={"total_tokens": 1, "cost": 0.0},
                    )
                )

        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("deadline-cas", budget_cost=10, deadline_seconds=0.04), [NodeSpec("slow", "slow", max_attempts=1)])
            result = scheduler.execute(SlowValidationAdapter())
            self.assertEqual(result.status, "cancelled")
            attempt = scheduler._conn.execute("SELECT status FROM attempts WHERE run_id='deadline-cas'").fetchone()
            self.assertEqual(attempt["status"], "failed")
            release = scheduler._conn.execute(
                "SELECT amount FROM budget_ledger WHERE run_id='deadline-cas' AND operation='release'"
            ).fetchone()
            self.assertIsNotNone(release)
            self.assertEqual(scheduler._conn.execute("SELECT reserved_cost FROM runs WHERE id='deadline-cas'").fetchone()[0], 0.0)

    def test_run_deadline_detaches_noncooperative_worker(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(
                RunSpec("deadline", budget_cost=10, deadline_seconds=0.03, max_workers=1),
                [NodeSpec("slow", "slow", timeout_seconds=1.0)],
            )
            adapter = LeaseAdapter()
            started = time.perf_counter()
            result = scheduler.execute(adapter)
            elapsed = time.perf_counter() - started
            self.assertEqual(result.status, "cancelled")
            self.assertLess(elapsed, 0.50)
            self.assertEqual(scheduler.snapshot("deadline")[0]["status"], "cancelled")

    def test_heartbeat_extends_node_lease(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("heartbeat", budget_cost=10), [NodeSpec("long", "long", timeout_seconds=0.03)])
            adapter = HeartbeatAdapter()
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertTrue(adapter.heartbeat_accepted)

    def test_run_owner_lease_prevents_duplicate_scheduler_execution(self):
        directory = self._run_dir()
        db_path = Path(directory) / "run.sqlite3"
        first = DAGScheduler(db_path)
        second = DAGScheduler(db_path)
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        first.submit(RunSpec("owned", budget_cost=10), [NodeSpec("n", "n")])
        adapter = LeaseAdapter()
        holder = []
        thread = threading.Thread(target=lambda: holder.append(first.execute(adapter, "owned")))
        thread.start()
        self.assertTrue(adapter.started.wait(1.0))
        self.assertEqual(second.recover_stale("owned", now=time.time(), owner_id=second._owner_id), ())
        with self.assertRaisesRegex(RuntimeError, "already being executed"):
            second.execute(CountingAdapter(), "owned")
        thread.join(2.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(holder[0].status, "succeeded")

    def test_same_scheduler_rejects_concurrent_run_reentry(self):
        class BlockingAdapter:
            def __init__(self):
                self.started = threading.Event()
                self.release = threading.Event()

            def run(self, node, context):
                self.started.set()
                self.release.wait(1.0)
                return WorkerResult({
                    "answer": "ok",
                    "claims": [],
                    "citations": [],
                    "confidence": 0.8,
                    "warnings": [],
                    "usage": {"total_tokens": 1, "cost": 0.0001},
                })

        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("same-instance", budget_cost=10), [NodeSpec("n", "n")])
            adapter = BlockingAdapter()
            holder = []
            thread = threading.Thread(target=lambda: holder.append(scheduler.execute(adapter, "same-instance")))
            thread.start()
            self.assertTrue(adapter.started.wait(1.0))
            try:
                with self.assertRaisesRegex(RuntimeError, "already being executed"):
                    scheduler.execute(CountingAdapter(), "same-instance")
            finally:
                adapter.release.set()
                thread.join(2.0)
            self.assertFalse(thread.is_alive())
            self.assertEqual(holder[0].status, "succeeded")

    def test_execute_setup_failure_releases_owner_claim(self):
        directory = self._run_dir()
        artifact_parent = directory / "run-artifacts"
        artifact_parent.write_text("not a directory", encoding="utf-8")
        scheduler = self.scheduler(directory)
        scheduler.submit(RunSpec("setup-failure", budget_cost=10), [NodeSpec("n", "n")])
        with self.assertRaises(OSError):
            scheduler.execute(CountingAdapter(), "setup-failure")
        scheduler.close()

    def test_close_rejects_active_execute(self):
        directory = self._run_dir()
        scheduler = self.scheduler(directory)
        scheduler.submit(RunSpec("close-active", budget_cost=10), [NodeSpec("n", "n")])
        adapter = LeaseAdapter()
        holder = []
        thread = threading.Thread(target=lambda: holder.append(scheduler.execute(adapter, "close-active")))
        thread.start()
        self.assertTrue(adapter.started.wait(1.0))
        with self.assertRaisesRegex(RuntimeError, "execute is active"):
            scheduler.close()
        thread.join(2.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(holder[0].status, "succeeded")
        scheduler.close()

    def test_cancel_running_work_cannot_commit_late_result(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("cancel", budget_cost=10), [NodeSpec("running", "running", max_attempts=2)])
            adapter = CancellationAdapter()
            holder = []
            thread = threading.Thread(target=lambda: holder.append(scheduler.execute(adapter, "cancel")))
            thread.start()
            self.assertTrue(adapter.started.wait(1.0))
            scheduler.cancel("cancel")
            thread.join(2.0)
            self.assertFalse(thread.is_alive())
            self.assertTrue(adapter.stopped.wait(1.0))
            self.assertEqual(holder[0].status, "cancelled")
            self.assertEqual(scheduler.snapshot("cancel")[0]["status"], "cancelled")
            self.assertEqual(scheduler._conn.execute("SELECT status FROM attempts WHERE run_id='cancel'").fetchone()[0], "failed")
            again = scheduler.execute(CountingAdapter(), "cancel")
            self.assertEqual(again.events, holder[0].events)

    def test_reverse_wave_and_invalid_numeric_inputs_are_rejected(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            with self.assertRaisesRegex(ValueError, "later wave"):
                scheduler.submit(RunSpec("reverse"), [NodeSpec("early", "early", wave=0, depends_on=("late",)), NodeSpec("late", "late", wave=1)])
            with self.assertRaises(ValueError):
                scheduler.submit(RunSpec("nan", budget_cost=float("nan")), [NodeSpec("n", "n")])
            with self.assertRaises(ValueError):
                scheduler.submit(RunSpec("bool-workers", max_workers=True), [NodeSpec("n", "n")])
            for reserved in ("CON", "NUL.txt", "COM1", "LPT9", "a*b", "a< b", "a|b"):
                with self.assertRaisesRegex(ValueError, "portable identifier"):
                    scheduler.submit(RunSpec(reserved), [NodeSpec("n", "n")])
            with self.assertRaisesRegex(ValueError, "portable identifier"):
                scheduler.submit(RunSpec("x" * 101), [NodeSpec("n", "n")])
            scheduler.submit(RunSpec("CaseSafe"), [NodeSpec("n", "n")])
            with self.assertRaisesRegex(ValueError, "case-insensitively"):
                scheduler.submit(RunSpec("casesafe"), [NodeSpec("n", "n")])

    def test_typed_routing_config_is_validated(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            with self.assertRaisesRegex(ValueError, "defer_excess"):
                scheduler.submit(
                    RunSpec("bad-stage", stage_policies=(StagePolicy("scout", defer_excess=1),)),
                    [NodeSpec("n", "n", stage="scout")],
                )
            with self.assertRaisesRegex(ValueError, "route profile id"):
                scheduler.submit(
                    RunSpec("bad-route", route_profiles=(RouteProfile("r", "p", "m", reasoning_effort=None),)),
                    [NodeSpec("n", "n")],
                )
            with self.assertRaisesRegex(ValueError, "unknown preferred route"):
                scheduler.submit(
                    RunSpec(
                        "unknown-preferred",
                        stage_policies=(StagePolicy("scout", preferred_routes=("missing",)),),
                        route_profiles=(RouteProfile("declared", "p", "m"),),
                    ),
                    [NodeSpec("n", "n", stage="scout")],
                )

    def test_scheduler_limit_overrides_run_limit(self):
        directory = self._run_dir()
        with DAGScheduler(Path(directory) / "run.sqlite3", max_workers=1) as scheduler:
            scheduler.submit(RunSpec("limit", budget_cost=10, max_workers=4), [NodeSpec(f"n{i}", "n") for i in range(4)])
            adapter = CountingAdapter(delay=0.02)
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertLessEqual(adapter.max_active, 1)

    def test_budget_settlement_does_not_double_count_current_reservation(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("budget-settle", budget_cost=1, cost_per_1k_tokens=1, max_workers=1), [NodeSpec("n", "n", max_tokens=1000)])
            adapter = CountingAdapter()
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertAlmostEqual(result.spent_cost, 0.001)

    def test_result_includes_persisted_events(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("events", budget_cost=10), [NodeSpec("n", "n")])
            scheduler.execute(CountingAdapter())
            result = scheduler.result("events")
            self.assertTrue(any(event.startswith("run.created") for event in result.events))
            self.assertTrue(any(event.startswith("run.completed") for event in result.events))

    def test_artifact_store_validates_digest_size_and_path(self):
        directory = self._run_dir()
        store = ArtifactStore(directory / "artifacts", max_bytes=8)
        ref, digest = store.put_text("中文")
        self.assertEqual(store.read_text(ref), "中文")
        with self.assertRaises(ValueError):
            store.put_text("too large")
        with self.assertRaises(KeyError):
            store.read_text("artifact:../escape")
        (directory / "artifacts" / digest).write_bytes(b"bad")
        with self.assertRaises(IOError):
            store.read_text(ref)

    def test_artifact_bindings_allow_same_digest_per_node_and_run(self):
        directory = self._run_dir()

        class SameArtifactAdapter(CountingAdapter):
            def run(self, node, context):
                base = super().run(node, context)
                return WorkerResult(base.envelope, {"same": "identical"})

        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("same", budget_cost=10), [NodeSpec("a", "a"), NodeSpec("b", "b")])
            result = scheduler.execute(SameArtifactAdapter())
            self.assertEqual(result.status, "succeeded")
            count = scheduler._conn.execute("SELECT COUNT(*) FROM artifacts WHERE run_id='same'").fetchone()[0]
            self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main()
