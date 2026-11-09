import math
from pathlib import Path
import tempfile
import threading
import unittest

from sparemesh import ConflictError, Coordinator, IdentityError, ValidationError


class Clock:
    def __init__(self):
        self.now = 1_700_000_000.0

    def __call__(self):
        return self.now


def snapshot(sequence=1, **changes):
    value = {
        "schema_version": 1,
        "sequence": sequence,
        "os": "darwin",
        "arch": "arm64",
        "runtimes": ["python3.11"],
        "profiles": ["smoke-tests"],
        "available_memory_mb": 2048,
        "ready": True,
        "allow_guests": True,
        "pressure": False,
    }
    value.update(changes)
    return value


def result(outcome="succeeded", exit_code=0, summary="done"):
    return {
        "schema_version": 1,
        "outcome": outcome,
        "exit_code": exit_code,
        "summary": summary,
    }


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "state.sqlite"
        self.clock = Clock()
        self.coordinator = Coordinator(self.path, clock=self.clock)
        self.addCleanup(self.coordinator.close)

    def host(self, host_id="alice-mac", owner="alice", **changes):
        worker_id = "worker-" + host_id
        self.coordinator.register_host(host_id, owner, worker_id)
        self.coordinator.report_host(host_id, worker_id, snapshot(**changes))
        return worker_id

    def submit(self, job_id="job-1", owner="alice", **changes):
        payload = {
            "job_id": job_id,
            "owner": owner,
            "profile": "smoke-tests",
            "revision": "a" * 40,
            "requirements": {
                "os": "darwin",
                "arch": "arm64",
                "runtime": "python3.11",
                "memory_mb": 512,
            },
            "restartable": False,
        }
        payload.update(changes)
        return self.coordinator.submit(**payload)

    def test_submission_is_durable_and_idempotent(self):
        first = self.submit()
        repeated = self.submit()
        self.assertEqual(first, repeated)
        self.coordinator.close()
        self.coordinator = Coordinator(self.path, clock=self.clock)
        self.addCleanup(self.coordinator.close)
        self.assertEqual(self.coordinator.status("job-1"), first)

    def test_submission_rejects_changed_immutable_payload(self):
        self.submit()
        with self.assertRaises(ConflictError):
            self.submit(restartable=True)

    def test_claim_prefers_submitters_eligible_host(self):
        self.host()
        bob_worker = self.host("bob-mac", "bob")
        self.submit()
        self.assertIsNone(self.coordinator.claim("bob-mac", bob_worker))
        assignment = self.coordinator.claim("alice-mac", "worker-alice-mac")
        self.assertEqual(assignment["job"]["job_id"], "job-1")

    def test_guest_routes_automatically_when_own_host_is_unavailable(self):
        self.host(ready=False)
        worker = self.host("bob-mac", "bob")
        self.submit()
        assignment = self.coordinator.claim("bob-mac", worker)
        self.assertEqual(assignment["job"]["owner"], "alice")

    def test_host_owner_priority_even_if_guest_was_queued_first(self):
        worker = self.host("bob-mac", "bob")
        self.submit("guest", "alice")
        self.submit("owner", "bob")
        self.assertIn("owner_priority", self.coordinator.status("guest")["waiting"][0]["reasons"])
        assignment = self.coordinator.claim("bob-mac", worker)
        self.assertEqual(assignment["job"]["job_id"], "owner")
        waiting = self.coordinator.status("guest")["waiting"]
        self.assertIn("occupied", waiting[0]["reasons"])

    def test_owner_submission_waits_for_running_guest_without_automatic_reclaim(self):
        worker = self.host("bob-mac", "bob")
        self.submit("guest", "alice", restartable=True)
        guest = self.coordinator.claim("bob-mac", worker)
        self.submit("owner", "bob")
        repeated = self.coordinator.claim("bob-mac", worker)
        self.assertEqual(repeated["attempt_id"], guest["attempt_id"])
        self.assertFalse(repeated["cancel_requested"])
        self.coordinator.finish("bob-mac", worker, guest["attempt_id"], result())
        self.assertEqual(self.coordinator.claim("bob-mac", worker)["job"]["job_id"], "owner")

    def test_claim_retry_returns_same_assignment_after_restart(self):
        worker = self.host()
        self.submit()
        first = self.coordinator.claim("alice-mac", worker)
        self.coordinator.close()
        self.coordinator = Coordinator(self.path, clock=self.clock)
        self.addCleanup(self.coordinator.close)
        self.assertEqual(self.coordinator.claim("alice-mac", worker), first)

    def test_concurrent_hosts_cannot_claim_the_same_job(self):
        self.host("one", "bob")
        self.host("two", "carol")
        self.submit()
        barrier = threading.Barrier(2)
        assignments = []
        failures = []

        def claim(host_id):
            try:
                with Coordinator(self.path, clock=self.clock) as coordinator:
                    barrier.wait(timeout=5)
                    assignments.append(coordinator.claim(host_id, "worker-" + host_id))
            except Exception as error:
                failures.append(error)

        threads = [threading.Thread(target=claim, args=(name,)) for name in ("one", "two")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(failures, [])
        self.assertEqual(sum(value is not None for value in assignments), 1)

    def test_concurrent_claims_on_same_host_return_one_attempt(self):
        self.host()
        self.submit("one")
        self.submit("two")
        barrier = threading.Barrier(2)
        assignments = []
        failures = []

        def claim():
            try:
                with Coordinator(self.path, clock=self.clock) as coordinator:
                    barrier.wait(timeout=5)
                    assignments.append(coordinator.claim("alice-mac", "worker-alice-mac"))
            except Exception as error:
                failures.append(error)

        threads = [threading.Thread(target=claim) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(failures, [])
        self.assertEqual(len(assignments), 2)
        self.assertEqual(assignments[0]["attempt_id"], assignments[1]["attempt_id"])

    def test_stale_host_is_not_scheduled(self):
        worker = self.host()
        self.submit()
        self.clock.now += 31
        self.assertIsNone(self.coordinator.claim("alice-mac", worker))
        self.assertIn("stale", self.coordinator.status("job-1")["waiting"][0]["reasons"])

    def test_stale_occupied_host_keeps_its_slot(self):
        worker = self.host()
        self.submit("one")
        first = self.coordinator.claim("alice-mac", worker)
        self.submit("two")
        self.clock.now += 300
        self.coordinator.report_host("alice-mac", worker, snapshot(sequence=2))
        self.assertEqual(self.coordinator.claim("alice-mac", worker)["attempt_id"], first["attempt_id"])
        self.assertEqual(self.coordinator.status("two")["state"], "queued")
        self.assertEqual(self.coordinator.hosts()[0]["active_attempt_id"], first["attempt_id"])

    def test_clock_rollback_closes_intake(self):
        worker = self.host()
        self.submit()
        self.clock.now -= 1
        self.assertIsNone(self.coordinator.claim("alice-mac", worker))

    def test_snapshot_replay_does_not_extend_freshness(self):
        worker = self.host()
        self.clock.now += 31
        self.coordinator.report_host("alice-mac", worker, snapshot())
        self.submit()
        self.assertIsNone(self.coordinator.claim("alice-mac", worker))
        with self.assertRaises(ConflictError):
            self.coordinator.report_host("alice-mac", worker, snapshot(ready=False))
        self.coordinator.report_host("alice-mac", worker, snapshot(sequence=2))
        with self.assertRaises(ConflictError):
            self.coordinator.report_host("alice-mac", worker, snapshot())

    def test_queued_cancellation_is_terminal_without_an_attempt(self):
        self.submit()
        state = self.coordinator.cancel("job-1", "alice")
        self.assertEqual(state["state"], "canceled")
        self.assertIsNone(state["attempt"])
        self.host()
        self.assertIsNone(self.coordinator.claim("alice-mac", "worker-alice-mac"))

    def test_running_cancel_holds_slot_until_confirmed_finish(self):
        worker = self.host()
        self.submit("one")
        first = self.coordinator.claim("alice-mac", worker)
        self.submit("two")
        self.coordinator.cancel("one", "alice")
        repeat = self.coordinator.claim("alice-mac", worker)
        self.assertEqual(repeat["attempt_id"], first["attempt_id"])
        self.assertTrue(repeat["cancel_requested"])
        self.coordinator.finish("alice-mac", worker, first["attempt_id"], result("canceled", None))
        self.assertEqual(self.coordinator.claim("alice-mac", worker)["job"]["job_id"], "two")

    def test_completion_retry_is_idempotent_even_after_next_assignment(self):
        worker = self.host()
        self.submit("one")
        first = self.coordinator.claim("alice-mac", worker)
        terminal = self.coordinator.finish("alice-mac", worker, first["attempt_id"], result())
        self.submit("two")
        second = self.coordinator.claim("alice-mac", worker)
        self.assertEqual(self.coordinator.finish("alice-mac", worker, first["attempt_id"], result()), terminal)
        self.assertEqual(self.coordinator.hosts()[0]["active_attempt_id"], second["attempt_id"])
        with self.assertRaises(ConflictError):
            self.coordinator.finish("alice-mac", worker, first["attempt_id"], result("failed", 1))
        self.assertEqual(self.coordinator.status("one")["state"], "succeeded")
        with self.assertRaises(ConflictError):
            self.submit("one", revision="b" * 40)

    def test_wrong_worker_cannot_claim_report_or_complete(self):
        worker = self.host()
        self.submit()
        with self.assertRaises(IdentityError):
            self.coordinator.claim("alice-mac", "imposter")
        with self.assertRaises(IdentityError):
            self.coordinator.report_host("alice-mac", "imposter", snapshot(sequence=2))
        first = self.coordinator.claim("alice-mac", worker)
        with self.assertRaises(IdentityError):
            self.coordinator.finish("alice-mac", "imposter", first["attempt_id"], result())
        self.host("bob-mac", "bob")
        with self.assertRaises(IdentityError):
            self.coordinator.finish("bob-mac", "worker-bob-mac", first["attempt_id"], result())
        self.assertEqual(self.coordinator.status("job-1")["state"], "assigned")

    def test_invalid_terminal_report_keeps_slot_reserved(self):
        worker = self.host()
        self.submit()
        assignment = self.coordinator.claim("alice-mac", worker)
        for report in ([], result(exit_code=True), result(outcome="unknown"), result(exit_code=1),
                       {**result(), "extra": "field"}):
            with self.subTest(report=report):
                with self.assertRaises(ValidationError):
                    self.coordinator.finish("alice-mac", worker, assignment["attempt_id"], report)
                self.assertEqual(self.coordinator.hosts()[0]["active_attempt_id"], assignment["attempt_id"])

    def test_completion_can_win_a_cancellation_race(self):
        worker = self.host()
        self.submit()
        assignment = self.coordinator.claim("alice-mac", worker)
        self.coordinator.cancel("job-1", "alice")
        completed = self.coordinator.finish("alice-mac", worker, assignment["attempt_id"], result())
        self.assertEqual(completed["state"], "succeeded")
        self.assertIsNone(self.coordinator.hosts()[0]["active_attempt_id"])

    def test_host_registration_cannot_replace_bound_identity(self):
        self.host()
        with self.assertRaises(ConflictError):
            self.coordinator.register_host("alice-mac", "bob", "new-worker")

    def test_only_job_owner_can_cancel(self):
        self.submit()
        with self.assertRaises(IdentityError):
            self.coordinator.cancel("job-1", "bob")

    def test_reclaim_only_cancels_restartable_guests(self):
        worker = self.host("bob-mac", "bob")
        self.submit()
        first = self.coordinator.claim("bob-mac", worker)
        state = self.coordinator.pause_guests("bob-mac", "bob", reclaim=True)
        self.assertTrue(state["guests_paused"])
        self.assertFalse(self.coordinator.claim("bob-mac", worker)["cancel_requested"])
        self.coordinator.finish("bob-mac", worker, first["attempt_id"], result())
        self.coordinator.resume_guests("bob-mac", "bob")
        self.submit("restartable", restartable=True)
        self.coordinator.claim("bob-mac", worker)
        self.coordinator.pause_guests("bob-mac", "bob", reclaim=True)
        self.assertTrue(self.coordinator.claim("bob-mac", worker)["cancel_requested"])

    def test_pressure_requests_cancel_only_for_restartable_guest(self):
        worker = self.host("bob-mac", "bob")
        self.submit(restartable=True)
        first = self.coordinator.claim("bob-mac", worker)
        self.coordinator.report_host("bob-mac", worker, snapshot(sequence=2, pressure=True))
        repeat = self.coordinator.claim("bob-mac", worker)
        self.assertTrue(repeat["cancel_requested"])
        self.assertEqual(repeat["attempt_id"], first["attempt_id"])
        self.assertEqual(repeat["cancel_reason"], "host_pressure")

    def test_pressure_keeps_nonrestartable_guest_running(self):
        worker = self.host("bob-mac", "bob")
        self.submit()
        self.coordinator.claim("bob-mac", worker)
        self.coordinator.report_host("bob-mac", worker, snapshot(sequence=2, pressure=True))
        self.assertFalse(self.coordinator.claim("bob-mac", worker)["cancel_requested"])

    def test_compatibility_capacity_and_guest_policy_are_admission_checks(self):
        cases = [
            ({"os": "linux"}, "os_mismatch"),
            ({"arch": "x86_64"}, "arch_mismatch"),
            ({"runtimes": []}, "runtime_missing"),
            ({"profiles": []}, "profile_not_allowed"),
            ({"available_memory_mb": 128}, "insufficient_memory"),
            ({"ready": False}, "not_ready"),
            ({"pressure": True}, "pressure"),
            ({"allow_guests": False}, "guests_disabled"),
        ]
        self.submit()
        for sequence, (changes, reason) in enumerate(cases, start=1):
            with self.subTest(reason=reason):
                if sequence == 1:
                    self.coordinator.register_host("bob-mac", "bob", "worker-bob-mac")
                self.coordinator.report_host("bob-mac", "worker-bob-mac", snapshot(sequence, **changes))
                self.assertIsNone(self.coordinator.claim("bob-mac", "worker-bob-mac"))
                self.assertIn(reason, self.coordinator.status("job-1")["waiting"][0]["reasons"])

    def test_manual_guest_pause_survives_new_snapshot(self):
        worker = self.host("bob-mac", "bob")
        self.coordinator.pause_guests("bob-mac", "bob")
        self.coordinator.report_host("bob-mac", worker, snapshot(sequence=2))
        self.submit()
        self.assertIsNone(self.coordinator.claim("bob-mac", worker))
        self.coordinator.resume_guests("bob-mac", "bob")
        self.assertIsNotNone(self.coordinator.claim("bob-mac", worker))

    def test_invalid_inputs_raise_domain_errors(self):
        for memory in (0, -1, math.nan, math.inf, True, "512"):
            with self.subTest(memory=memory):
                with self.assertRaises(ValidationError):
                    self.submit(requirements={"os": "darwin", "arch": "arm64", "runtime": "python3.11", "memory_mb": memory})
        for invalid in ("", "contains spaces", None, 42):
            with self.subTest(job_id=invalid):
                with self.assertRaises(ValidationError):
                    self.submit(job_id=invalid)
        with self.assertRaises(ValidationError):
            self.submit(restartable="false")
        with self.assertRaises(ValidationError):
            self.submit(requirements=[])
        with self.assertRaises(ValidationError):
            self.submit(revision="main")
        self.coordinator.register_host("host", "owner", "worker")
        with self.assertRaises(ValidationError):
            self.coordinator.report_host("host", "worker", snapshot(ready=1))
        with self.assertRaises(ValidationError):
            self.coordinator.report_host("host", "worker", snapshot(available_memory_mb=math.nan))


if __name__ == "__main__":
    unittest.main()
