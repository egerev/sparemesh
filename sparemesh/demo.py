"""Deterministic coordinator demonstration, with no processes or network I/O."""

from pathlib import Path
from tempfile import TemporaryDirectory

from . import ConflictError, Coordinator


def run_demo() -> dict:
    now = 1_700_000_000.0
    checks = []

    def snapshot(sequence, *, ready=True):
        return {
            "schema_version": 1,
            "sequence": sequence,
            "os": "darwin",
            "arch": "arm64",
            "runtimes": ["python3.11"],
            "profiles": ["smoke-tests"],
            "available_memory_mb": 2048,
            "ready": ready,
            "allow_guests": True,
            "pressure": False,
        }

    def submit(coordinator, job_id, owner):
        return coordinator.submit(job_id, owner, "smoke-tests", "a" * 40, {
            "os": "darwin", "arch": "arm64", "runtime": "python3.11", "memory_mb": 512,
        })

    def finish(coordinator, attempt, outcome="succeeded"):
        return coordinator.finish(attempt["host_id"], attempt["worker_id"], attempt["attempt_id"], {
            "schema_version": 1,
            "outcome": outcome,
            "exit_code": 0 if outcome == "succeeded" else None,
            "summary": "simulated completion",
        })

    def check(event, condition, **details):
        if not condition:
            raise AssertionError(f"demo invariant failed: {event}")
        checks.append({"event": event, **details})

    with TemporaryDirectory(prefix="sparemesh-demo-") as directory:
        database = Path(directory) / "coordinator.sqlite3"
        with Coordinator(database, clock=lambda: now) as coordinator:
            coordinator.register_host("alice-mac", "alice", "worker-alice")
            coordinator.register_host("bob-mac", "bob", "worker-bob")
            coordinator.report_host("alice-mac", "worker-alice", snapshot(1))
            coordinator.report_host("bob-mac", "worker-bob", snapshot(1))

            submit(coordinator, "local", "alice")
            check("own_host_preferred", coordinator.claim("bob-mac", "worker-bob") is None)
            local = coordinator.claim("alice-mac", "worker-alice")
            check("local_run", local["job"]["job_id"] == "local", host=local["host_id"])
            finish(coordinator, local)

            coordinator.report_host("alice-mac", "worker-alice", snapshot(2, ready=False))
            submit(coordinator, "peer-alice", "alice")
            peer_alice = coordinator.claim("bob-mac", "worker-bob")
            check("peer_alice", peer_alice["job"]["job_id"] == "peer-alice", host=peer_alice["host_id"])
            finish(coordinator, peer_alice)

            coordinator.report_host("alice-mac", "worker-alice", snapshot(3))
            coordinator.report_host("bob-mac", "worker-bob", snapshot(2, ready=False))
            submit(coordinator, "peer-bob", "bob")
            peer_bob = coordinator.claim("alice-mac", "worker-alice")
            check("peer_bob", peer_bob["job"]["job_id"] == "peer-bob", host=peer_bob["host_id"])
            finish(coordinator, peer_bob)

            coordinator.report_host("alice-mac", "worker-alice", snapshot(4))
            coordinator.report_host("bob-mac", "worker-bob", snapshot(3))
            submit(coordinator, "alice-busy", "alice")
            submit(coordinator, "bob-busy", "bob")
            alice_busy = coordinator.claim("alice-mac", "worker-alice")
            bob_busy = coordinator.claim("bob-mac", "worker-bob")
            submit(coordinator, "waiting", "alice")
            waiting = coordinator.status("waiting")
            check("both_busy", all("occupied" in item["reasons"] for item in waiting["waiting"]),
                  waiting=waiting["waiting"])

            submit(coordinator, "cancel-queued", "alice")
            canceled = coordinator.cancel("cancel-queued", "alice")
            check("queued_cancel", canceled["state"] == "canceled" and canceled["attempt"] is None)
            coordinator.cancel("alice-busy", "alice")
            canceling = coordinator.claim("alice-mac", "worker-alice")
            check("running_cancel", canceling["cancel_requested"] and
                  canceling["attempt_id"] == alice_busy["attempt_id"], slot_retained=True)

        now += 31
        with Coordinator(database, clock=lambda: now) as coordinator:
            persisted = coordinator.hosts()
            check("restart_retains_slots", all(not host["fresh"] and host["active_attempt_id"] for host in persisted),
                  reserved_slots=len(persisted))
            coordinator.report_host("alice-mac", "worker-alice", snapshot(5))
            try:
                coordinator.report_host("alice-mac", "worker-alice", snapshot(4))
            except ConflictError:
                check("stale_snapshot_rejected", True)
            else:
                check("stale_snapshot_rejected", False)

            completed = finish(coordinator, alice_busy, "canceled")
            next_attempt = coordinator.claim("alice-mac", "worker-alice")
            check("confirmed_release", next_attempt["job"]["job_id"] == "waiting")
            replay = finish(coordinator, alice_busy, "canceled")
            check("completion_replay", replay == completed and
                  coordinator.claim("alice-mac", "worker-alice")["attempt_id"] == next_attempt["attempt_id"])
            try:
                finish(coordinator, alice_busy)
            except ConflictError:
                check("conflicting_completion_rejected", True)
            else:
                check("conflicting_completion_rejected", False)
            finish(coordinator, next_attempt)
            finish(coordinator, bob_busy)
            final_states = {job["job_id"]: job["state"] for job in coordinator.jobs()}
        return {"schema_version": 1, "mode": "simulated_workers", "checks": checks, "final_states": final_states}
