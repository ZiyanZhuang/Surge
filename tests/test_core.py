import shutil
import concurrent.futures
import sqlite3
import sys
import threading
import time
import unittest
import uuid
from unittest import mock
from contextlib import closing
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
    def test_failed_worker_dispatch_settles_attempts_without_orphan_calls(self):
        real_executor = concurrent.futures.ThreadPoolExecutor
        for after_enqueue in (False, True):
            with self.subTest(after_enqueue=after_enqueue):
                failed = threading.Event()
                pools = []

                class FaultyExecutor(real_executor):
                    submissions = 0

                    def submit(self, function, *args, **kwargs):
                        self.submissions += 1
                        if self.submissions != 2:
                            return super().submit(function, *args, **kwargs)
                        if after_enqueue:
                            super().submit(function, *args, **kwargs)
                        failed.set()
                        raise RuntimeError("injected worker launch failure")

                    def __init__(self, *args, **kwargs):
                        super().__init__(*args, **kwargs)
                        pools.append(self)

                class WaitingAdapter(CountingAdapter):
                    def run(self, node, context):
                        if node.id == "a":
                            if not failed.wait(2):
                                raise RuntimeError("second dispatch did not arrive")
                        return super().run(node, context)

                adapter = WaitingAdapter()
                error = None
                result = None
                with self.scheduler(self._run_dir()) as scheduler:
                    scheduler.submit(RunSpec("launch-failure", max_workers=2), [NodeSpec(name, name, max_attempts=1) for name in ("a", "b")])
                    try:
                        with mock.patch("surge_cluster.core.concurrent.futures.ThreadPoolExecutor", FaultyExecutor):
                            result = scheduler.execute(adapter, "launch-failure")
                    except RuntimeError as exc:
                        error = str(exc)
                    finally:
                        for pool in pools:
                            pool.shutdown(wait=True)
                    self.assertIsNone(error, error)
                    self.assertEqual(result.status, "failed")
                    self.assertEqual(adapter.calls, {"a": 1})
                    self.assertEqual(result.succeeded, ("a",))
                    self.assertEqual(result.failed, ("b",))
                    self.assertAlmostEqual(result.spent_cost, 0.001)
                    self.assertEqual(scheduler.budget_snapshot("launch-failure")["reserved_cost"], 0)
                    self.assertEqual(len(scheduler.events("launch-failure", "task.failed")), 1)
                    self.assertEqual(len(scheduler.events("launch-failure", "run.completed")), 1)

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

    def test_unknown_future_schema_is_rejected_without_changing_data(self):
        directory = self._run_dir()
        db = directory / "future.sqlite3"
        with closing(sqlite3.connect(db)) as connection, connection:
            connection.execute("PRAGMA user_version=99")
            connection.execute("CREATE TABLE artifacts (id TEXT PRIMARY KEY, future_note TEXT)")
            connection.executemany("INSERT INTO artifacts VALUES(?,?)", [("a", "retain A"), ("b", "retain B")])
            before = tuple(connection.iterdump())
        error = None
        try:
            with DAGScheduler(db):
                pass
        except ValueError as exc:
            error = str(exc)
        with closing(sqlite3.connect(db)) as connection, connection:
            after = tuple(connection.iterdump())
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        self.assertEqual(after, before, "unsupported schema data was rewritten")
        self.assertEqual(version, 99)
        self.assertIsNotNone(error)
        self.assertIn("schema version", error)

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

    def test_ambiguous_legacy_artifacts_are_not_silently_discarded(self):
        for mode in ("unbound", "duplicate", "null", "extra-column"):
            with self.subTest(mode=mode):
                db = self._run_dir() / "legacy-artifacts.sqlite3"
                with closing(sqlite3.connect(db)) as connection, connection:
                    if mode == "unbound":
                        connection.execute("CREATE TABLE artifacts(ref TEXT PRIMARY KEY,path TEXT,sha256 TEXT)")
                        connection.executemany("INSERT INTO artifacts VALUES(?,?,?)", [("a", "pa", "sa"), ("b", "pb", "sb")])
                    else:
                        connection.execute("CREATE TABLE artifacts(ref TEXT PRIMARY KEY,run_id TEXT,node_id TEXT,name TEXT,path TEXT,sha256 TEXT)")
                        connection.executemany("INSERT INTO artifacts VALUES(?,?,?,?,?,?)", [
                            ("a", "r", "n", "notes", "pa", "sa"),
                            ("b", "r", "n", "notes" if mode == "duplicate" else "other", None if mode == "null" else "pb", "sb"),
                        ])
                    if mode == "extra-column":
                        connection.execute("ALTER TABLE artifacts ADD COLUMN opaque_metadata TEXT")
                        connection.execute("UPDATE artifacts SET opaque_metadata='preserve legacy annotation'")
                    before = tuple(connection.iterdump())
                error = None
                try:
                    with DAGScheduler(db):
                        pass
                except ValueError as exc:
                    error = str(exc)
                with closing(sqlite3.connect(db)) as connection:
                    after = tuple(connection.iterdump())
                self.assertEqual(after, before, "legacy migration discarded artifact data")
                self.assertIsNotNone(error)
                self.assertIn("artifact", error)

    def test_legacy_artifact_migration_rechecks_rows_under_its_write_lock(self):
        db = self._run_dir() / "legacy-race.sqlite3"
        real_connect = sqlite3.connect
        with closing(real_connect(db)) as writer, writer:
            writer.execute("CREATE TABLE artifacts(opaque_payload TEXT)")
        inserted = False

        def connect_with_interleaved_writer(*args, **kwargs):
            connection = real_connect(*args, **kwargs)

            def insert_before_migration(statement):
                nonlocal inserted
                if statement == "BEGIN IMMEDIATE" and not inserted:
                    with closing(real_connect(db)) as writer, writer:
                        writer.execute("INSERT INTO artifacts VALUES(?)", ("must preserve opaque payload",))
                    inserted = True

            connection.set_trace_callback(insert_before_migration)
            return connection

        error = None
        with mock.patch("surge_cluster.core.sqlite3.connect", side_effect=connect_with_interleaved_writer):
            try:
                with DAGScheduler(db):
                    pass
            except ValueError as exc:
                error = str(exc)
        self.assertTrue(inserted)
        with closing(real_connect(db)) as reader:
            columns = {row[1] for row in reader.execute("PRAGMA table_info(artifacts)")}
            self.assertEqual(columns, {"opaque_payload"}, "migration dropped a concurrently inserted payload")
            self.assertEqual(reader.execute("SELECT opaque_payload FROM artifacts").fetchall(), [("must preserve opaque payload",)])
        self.assertIsNotNone(error)
        self.assertIn("legacy artifacts", error)

    def test_current_artifact_schema_preserves_extra_columns_without_rebuilding(self):
        db = self._run_dir() / "current-extra.sqlite3"
        with DAGScheduler(db):
            pass
        with closing(sqlite3.connect(db)) as connection, connection:
            connection.execute("ALTER TABLE artifacts ADD COLUMN opaque_metadata TEXT")
            connection.execute("INSERT INTO artifacts VALUES(?,?,?,?,?,?,?)", ("r", "n", "notes", "ref", "path", "sha", "keep annotation"))
        with DAGScheduler(db) as scheduler:
            self.assertEqual(scheduler._conn.execute("SELECT opaque_metadata FROM artifacts").fetchone()[0], "keep annotation")

    def test_unambiguous_legacy_artifact_bindings_are_preserved(self):
        db = self._run_dir() / "legacy-artifacts.sqlite3"
        records = [("r", "a", "notes", "ref-a", "path-a", "sha-a"), ("r", "b", "notes", "ref-b", "path-b", "sha-b")]
        with closing(sqlite3.connect(db)) as connection, connection:
            connection.execute("CREATE TABLE artifacts(run_id TEXT,node_id TEXT,name TEXT,ref TEXT PRIMARY KEY,path TEXT,sha256 TEXT)")
            connection.executemany("INSERT INTO artifacts VALUES(?,?,?,?,?,?)", records)
        with DAGScheduler(db) as scheduler:
            migrated = [tuple(row) for row in scheduler._conn.execute("SELECT run_id,node_id,name,ref,path,sha256 FROM artifacts ORDER BY node_id")]
            self.assertEqual(migrated, records)

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

    def test_completed_worker_is_not_timed_out_by_late_observation(self):
        original_wait = concurrent.futures.wait
        for hide_done in (False, True):
            with self.subTest(hide_done=hide_done):
                delayed = False

                def delayed_wait(*args, **kwargs):
                    nonlocal delayed
                    done, pending = original_wait(*args, **kwargs)
                    if done and not delayed:
                        delayed = True
                        time.sleep(0.08)
                        if hide_done:
                            return set(), pending | done
                    return done, pending

                directory = self._run_dir()
                with self.scheduler(directory) as scheduler:
                    scheduler.submit(RunSpec("observe"), [NodeSpec("n", "work", max_attempts=1, timeout_seconds=0.05)])
                    with mock.patch("surge_cluster.core.concurrent.futures.wait", side_effect=delayed_wait):
                        result = scheduler.execute(CountingAdapter())
                    self.assertEqual(result.status, "succeeded")

    def test_observed_completed_cost_is_retained_on_timeout_or_cancel(self):
        original_wait = concurrent.futures.wait
        for cancel in ("timeout", "local", "remote"):
            with self.subTest(cancel=cancel):
                directory = self._run_dir()
                with self.scheduler(directory) as scheduler:
                    scheduler.submit(RunSpec("paid-stop"), [NodeSpec("n", "work", max_attempts=1, timeout_seconds=0.02 if cancel == "timeout" else 5)])

                    def delayed_wait(futures, **kwargs):
                        done, pending = original_wait(futures, timeout=5)
                        if cancel != "timeout":
                            if cancel == "remote":
                                with self.scheduler(directory) as other:
                                    other.cancel("paid-stop")
                            else:
                                scheduler.cancel("paid-stop")
                            return set(), pending | done
                        return done, pending

                    with mock.patch("surge_cluster.core.concurrent.futures.wait", side_effect=delayed_wait):
                        result = scheduler.execute(CountingAdapter(delay=0.04))
                    self.assertEqual(result.status, "failed" if cancel == "timeout" else "cancelled")
                    self.assertAlmostEqual(result.spent_cost, 0.001)
                    attempt = scheduler._conn.execute("SELECT cost,status FROM attempts").fetchone()
                    self.assertEqual(attempt["status"], "failed")
                    self.assertAlmostEqual(attempt["cost"], 0.001)
                    self.assertEqual(scheduler.artifacts("paid-stop"), [])

    def test_cancel_without_a_live_owner_releases_reservation_once(self):
        directory = self._run_dir()
        with self.scheduler(directory) as scheduler:
            scheduler.submit(RunSpec("orphan-cancel"), [NodeSpec("n", "work")])
            with scheduler._conn:
                run = scheduler._get_run("orphan-cancel")
                node = scheduler._get_nodes("orphan-cancel")["n"]
                scheduler._reserve(run, node, "orphan")
                scheduler._conn.execute("UPDATE runs SET status='running',owner_id='gone',owner_lease_until=0 WHERE id='orphan-cancel'")
                scheduler._conn.execute("UPDATE nodes SET status='running',attempt_count=1,current_attempt_id='orphan' WHERE run_id='orphan-cancel'")
                scheduler._conn.execute("INSERT INTO attempts(id,run_id,node_id,number,status,lease_until,started_at) VALUES('orphan','orphan-cancel','n',1,'running',0,0)")
            scheduler.cancel("orphan-cancel")
            scheduler.cancel("orphan-cancel")
            self.assertEqual(scheduler._get_run("orphan-cancel")["reserved_cost"], 0)
            self.assertEqual(scheduler.snapshot("orphan-cancel")[0]["status"], "cancelled")
            self.assertEqual(scheduler._conn.execute("SELECT COUNT(*) FROM budget_ledger WHERE operation='release'").fetchone()[0], 1)

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

    def test_stale_claim_cannot_restart_a_completed_run(self):
        directory = self._run_dir()
        db_path = directory / "run.sqlite3"
        with DAGScheduler(db_path) as first, DAGScheduler(db_path) as second:
            first.submit(RunSpec("claim-race"), [NodeSpec("n", "work")])
            paused, release = threading.Event(), threading.Event()

            def pause_claim(statement):
                if "SET status='running', owner_id=" in statement:
                    paused.set()
                    release.wait(5)

            second._conn.set_trace_callback(pause_claim)
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(second.execute, CountingAdapter(), "claim-race")
                try:
                    self.assertTrue(paused.wait(2))
                    self.assertEqual(first.execute(CountingAdapter(), "claim-race").status, "succeeded")
                finally:
                    release.set()
                self.assertEqual(future.result(timeout=5).status, "succeeded")
            self.assertEqual(len(first.events("claim-race", "run.completed")), 1)

    def test_concurrent_scheduler_startups_wait_for_wal(self):
        errors = []
        for _ in range(10):
            db_path = self._run_dir() / "shared.sqlite3"
            barrier = threading.Barrier(8)

            def open_scheduler():
                barrier.wait(timeout=5)
                with DAGScheduler(db_path):
                    pass

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                futures = [pool.submit(open_scheduler) for _ in range(8)]
                errors.extend(str(future.exception()) for future in futures if future.exception() is not None)
        self.assertEqual(errors, [])

    @unittest.skipUnless(sys.platform == "win32", "Windows open-file cleanup semantics")
    def test_failed_constructor_releases_database_handle(self):
        db_path = self._run_dir() / "corrupt.sqlite3"
        db_path.write_bytes(b"not a SQLite file" * 100)
        errors = []
        try:
            DAGScheduler(db_path)
        except sqlite3.DatabaseError as exc:
            errors.append(exc)
        self.assertEqual(len(errors), 1)
        db_path.rename(db_path.with_name("released.sqlite3"))

    def test_submission_rejects_unrepresentable_numbers_atomically(self):
        cases = (
            (RunSpec("too-large", budget_cost=10 ** 400), NodeSpec("n", "work")),
            (RunSpec("too-large", max_workers=10 ** 100), NodeSpec("n", "work")),
            (RunSpec("too-large"), NodeSpec("n", "work", max_tokens=10 ** 100)),
        )
        for run, node in cases:
            with self.subTest(run=run, node=node):
                with self.scheduler(self._run_dir()) as scheduler:
                    with self.assertRaises(ValueError):
                        scheduler.submit(run, [node])
                    with self.assertRaises(KeyError):
                        scheduler.result(run.id)

    def test_control_api_rejects_unrepresentable_numbers_as_value_errors(self):
        with self.scheduler(self._run_dir()) as scheduler:
            for operation in (
                lambda: scheduler.heartbeat("missing", "n", "a", 10 ** 400),
                lambda: scheduler.report_progress("missing", "n", "a", 10 ** 400),
                lambda: scheduler.recover_stale("missing", now=10 ** 400),
            ):
                with self.subTest(operation=operation), self.assertRaises(ValueError):
                    operation()

    def test_finite_integer_values_bind_as_sqlite_real_fields(self):
        for run_fields, node_fields in (
            ({"budget_cost": 10 ** 100}, {}),
            ({"cost_per_1k_tokens": 10 ** 100}, {}),
            ({}, {"timeout_seconds": 10 ** 100}),
            ({}, {"incremental_value": 10 ** 100}),
        ):
            with self.subTest(run_fields=run_fields, node_fields=node_fields), self.scheduler(self._run_dir()) as scheduler:
                run = RunSpec("finite-real", **run_fields)
                node = NodeSpec("n", "work", **node_fields)
                scheduler.submit(run, [node])
                row = scheduler._conn.execute("SELECT budget_cost,cost_per_1k_tokens FROM runs").fetchone()
                self.assertEqual(tuple(row), (float(run.budget_cost), float(run.cost_per_1k_tokens)))
                row = scheduler._conn.execute("SELECT timeout_seconds,incremental_value,urgency FROM nodes").fetchone()
                self.assertEqual(tuple(row), (float(node.timeout_seconds), float(node.incremental_value), float(node.urgency)))

    def test_finite_integer_recovery_timestamp_is_bound_as_real(self):
        with self.scheduler(self._run_dir()) as scheduler:
            scheduler.submit(RunSpec("real-timestamp"), [NodeSpec("n", "work")])
            self.assertEqual(scheduler.recover_stale("real-timestamp", now=10 ** 100), ())

    def test_overflowing_stage_totals_are_rejected_before_persistence(self):
        for stage in ("", "selected"):
            with self.subTest(stage=stage), self.scheduler(self._run_dir()) as scheduler:
                policy = StagePolicy(stage or "wave-0", target_progress=0.9)
                nodes = [NodeSpec(f"n{i}", "work", stage=stage, incremental_value=1e308) for i in range(3)]
                with self.assertRaisesRegex(ValueError, "incremental value"):
                    scheduler.submit(RunSpec("overflow-stage", stage_policies=(policy,)), nodes)
                with self.assertRaises(KeyError):
                    scheduler.result("overflow-stage")

    def test_deep_dag_validation_does_not_use_the_python_call_stack(self):
        for cyclic in (False, True):
            with self.subTest(cyclic=cyclic):
                nodes = [NodeSpec(f"n{i}", "work", depends_on=(f"n{i + 1}",) if i < 1199 else ()) for i in range(1200)]
                if cyclic:
                    nodes[-1] = NodeSpec("n1199", "work", depends_on=("n0",))
                with self.scheduler(self._run_dir()) as scheduler:
                    if cyclic:
                        with self.assertRaisesRegex(ValueError, "cycle"):
                            scheduler.submit(RunSpec("deep", max_nodes=1200), nodes)
                    else:
                        scheduler.submit(RunSpec("deep", max_nodes=1200), nodes)
                        self.assertEqual(len(scheduler.snapshot("deep")), 1200)

    def test_busy_affordable_route_waits_instead_of_blocking(self):
        with self.scheduler(self._run_dir()) as scheduler:
            scheduler.submit(
                RunSpec(
                    "route-intersection", budget_cost=0.003, max_workers=2,
                    route_profiles=(
                        RouteProfile("expensive", "p", "large", cost_per_1k_tokens=0.01, max_concurrency=1),
                        RouteProfile("cheap", "p", "small", cost_per_1k_tokens=0.001, max_concurrency=1),
                    ),
                ),
                [NodeSpec(f"n{i}", "work", max_tokens=1000) for i in range(2)],
            )
            adapter = RoutingAdapter(delay=0.05)
            result = scheduler.execute(adapter)
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(result.succeeded, ("n0", "n1"))
            self.assertEqual(adapter.max_active, 1)

    def test_artifact_names_cannot_impersonate_digest_references(self):
        fake_ref = "artifact:" + "0" * 64

        class DigestNamedAdapter(CountingAdapter):
            def run(self, node, context):
                base = super().run(node, context)
                envelope = dict(base.envelope)
                envelope["claims"] = [{"id": "c", "text": "value", "evidence_refs": [fake_ref]}]
                return WorkerResult(envelope, {fake_ref: "99"})

        with self.scheduler(self._run_dir()) as scheduler:
            scheduler.submit(RunSpec("fake-digest"), [NodeSpec("n", "work", max_attempts=1)])
            self.assertEqual(scheduler.execute(DigestNamedAdapter()).status, "failed")
            self.assertEqual(scheduler.artifacts("fake-digest"), [])

    def test_ambiguous_qualified_dependency_alias_is_rejected(self):
        class AmbiguousAdapter(CountingAdapter):
            def run(self, node, context):
                base = super().run(node, context)
                if node.id == "a/b":
                    return WorkerResult(base.envelope, {"c": "left source"})
                if node.id == "a":
                    return WorkerResult(base.envelope, {"b/c": "right source"})
                envelope = dict(base.envelope)
                envelope["claims"] = [{"id": "c", "text": "value", "evidence_refs": ["a/b/c"]}]
                return WorkerResult(envelope)

        with self.scheduler(self._run_dir()) as scheduler:
            scheduler.submit(RunSpec("ambiguous-alias"), [NodeSpec("a/b", "work"), NodeSpec("a", "work"), NodeSpec("sink", "work", depends_on=("a/b", "a"), wave=1, max_attempts=1)])
            result = scheduler.execute(AmbiguousAdapter())
            self.assertEqual(result.failed, ("sink",))

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

    def test_terminal_execute_returns_the_same_persisted_events(self):
        for invalid in (False, True):
            with self.subTest(invalid=invalid):
                directory = self._run_dir()
                with self.scheduler(directory) as scheduler:
                    scheduler.submit(RunSpec("stable-events"), [NodeSpec("n", "work", max_attempts=1)])
                    first = scheduler.execute(CountingAdapter(invalid=invalid), "stable-events")
                    second = scheduler.execute(CountingAdapter(), "stable-events")
                    self.assertEqual(first.events, second.events)
                    self.assertEqual(first.events, scheduler.result("stable-events").events)

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

    def test_disabling_deferral_finishes_all_stage_nodes(self):
        for target in (0.0, 0.5):
            with self.subTest(target=target):
                directory = self._run_dir()
                with self.scheduler(directory) as scheduler:
                    scheduler.submit(
                        RunSpec(
                            "no-deferral", deadline_seconds=0.3,
                            stage_policies=(StagePolicy("work", target_progress=target, defer_excess=False),),
                        ),
                        [NodeSpec(f"n{i}", "work", stage="work") for i in range(2)],
                    )
                    result = scheduler.execute(CountingAdapter())
                    self.assertEqual(result.status, "succeeded")
                    self.assertEqual(result.succeeded, ("n0", "n1"))
                    self.assertEqual(result.deferred, ())

    def test_full_stage_target_finishes_zero_value_nodes(self):
        for values in ((0.0, 0.0), (1.0, 0.0)):
            with self.subTest(values=values):
                directory = self._run_dir()
                with self.scheduler(directory) as scheduler:
                    scheduler.submit(
                        RunSpec("zero-value", deadline_seconds=0.3, stage_policies=(StagePolicy("work"),)),
                        [NodeSpec(f"n{i}", "work", stage="work", incremental_value=value) for i, value in enumerate(values)],
                    )
                    result = scheduler.execute(CountingAdapter())
                    self.assertEqual(result.status, "succeeded")
                    self.assertEqual(result.succeeded, ("n0", "n1"))

    def test_artifact_write_near_windows_path_limit(self):
        directory = self._run_dir().resolve()
        root = directory / ("p" * max(1, 190 - len(str(directory)) - 1))
        store = ArtifactStore(root)
        ref, _ = store.put_text("long-path evidence")
        self.assertEqual(store.read_text(ref), "long-path evidence")

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
