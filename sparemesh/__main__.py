"""Local operator CLI. Run from the checkout with python3 -m sparemesh."""

import argparse
import json
from pathlib import Path
import sqlite3
import sys
import time

from . import Coordinator, CoordinatorError, ValidationError
from .demo import run_demo
from .source_bundle import SourceBundleError, package_source
from .pilot import Pilot, load_config
from .transport import TransportError


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sparemesh local coordinator prototype")
    parser.add_argument("--db", default="sparemesh.sqlite3", help="local SQLite file (default: sparemesh.sqlite3)")
    commands = parser.add_subparsers(dest="command", required=True)

    register = commands.add_parser("register-host", help="register an immutable host/worker/owner binding")
    register.add_argument("host_id")
    register.add_argument("--owner", required=True)
    register.add_argument("--worker", required=True)

    report = commands.add_parser("report-host", help="accept a sequenced worker snapshot")
    report.add_argument("host_id")
    report.add_argument("--worker", required=True)
    report.add_argument("--snapshot", required=True, help="JSON file, or - for stdin")

    submit = commands.add_parser("submit", help="enqueue a named worker profile")
    submit.add_argument("profile")
    submit.add_argument("--job-id", required=True, help="stable client-generated idempotency ID")
    submit.add_argument("--owner", required=True)
    submit.add_argument("--ref", required=True, dest="revision", help="full commit ID")
    submit.add_argument("--os", required=True)
    submit.add_argument("--arch", required=True)
    submit.add_argument("--runtime", required=True)
    submit.add_argument("--memory-mb", required=True, type=float)
    submit.add_argument("--restartable", action="store_true", help="permit host reclaim/pressure cancellation")

    claim = commands.add_parser("claim", help="pull the current or next assignment for one worker")
    claim.add_argument("host_id")
    claim.add_argument("--worker", required=True)

    status = commands.add_parser("status", help="show a job with per-host waiting reasons")
    status.add_argument("job_id")
    jobs = commands.add_parser("jobs", help="list jobs in submission order")
    jobs.add_argument("--owner")
    commands.add_parser("hosts", help="list host snapshots and reserved slots")

    cancel = commands.add_parser("cancel", help="cancel a queued job or request cancellation of an attempt")
    cancel.add_argument("job_id")
    cancel.add_argument("--owner", required=True)

    pause = commands.add_parser("pause-guests", help="close a host's guest intake")
    pause.add_argument("host_id")
    pause.add_argument("--owner", required=True)
    pause.add_argument("--reclaim", action="store_true", help="request cancellation of a restartable guest")
    resume = commands.add_parser("resume-guests", help="remove the owner's manual guest pause")
    resume.add_argument("host_id")
    resume.add_argument("--owner", required=True)

    finish = commands.add_parser("finish", help="reconcile a worker's terminal attempt result")
    finish.add_argument("attempt_id")
    finish.add_argument("--host", required=True)
    finish.add_argument("--worker", required=True)
    finish.add_argument("--result", required=True, help="JSON file, or - for stdin")

    commands.add_parser("demo", help="run simulated workers in an isolated temporary database")

    bundle = commands.add_parser("bundle-source", help="package one exact Git commit for a worker")
    bundle.add_argument("--repo", required=True, help="local Git repository path")
    bundle.add_argument("--commit", required=True, help="full commit object ID, not a branch or tag")
    bundle.add_argument("--output", required=True, help="new bundle directory under an existing parent")

    pilot_submit = commands.add_parser("pilot-submit", help="package a commit and enqueue it for real workers")
    pilot_submit.add_argument("--config", required=True, help="pilot JSON configuration")
    pilot_submit.add_argument("--repo", required=True, help="local Git repository path")
    pilot_submit.add_argument("--ref", required=True, dest="revision", help="full commit object ID")
    pilot_submit.add_argument("--profile", required=True)
    pilot_submit.add_argument("--job-id", required=True)
    pilot_submit.add_argument("--owner", required=True)
    pilot_submit.add_argument("--os", required=True)
    pilot_submit.add_argument("--arch", required=True)
    pilot_submit.add_argument("--runtime", required=True)
    pilot_submit.add_argument("--memory-mb", required=True, type=float)
    pilot_submit.add_argument("--restartable", action="store_true")

    for command, help_text in (("pilot-tick", "probe, dispatch, and reconcile every configured worker once"),
                               ("pilot-run", "run the worker controller until interrupted"),
                               ("pilot-result", "read a job result and its worker log tails")):
        pilot_command = commands.add_parser(command, help=help_text)
        pilot_command.add_argument("--config", required=True, help="pilot JSON configuration")
        if command == "pilot-run":
            pilot_command.add_argument("--max-ticks", type=int, help="stop after this many ticks")
        elif command == "pilot-result":
            pilot_command.add_argument("--job-id", required=True)
    return parser


def _read_json(filename: str):
    try:
        text = sys.stdin.read() if filename == "-" else Path(filename).read_text(encoding="utf-8")
        return json.loads(text)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValidationError(f"cannot read JSON input: {error}") from error


def _dispatch(coordinator: Coordinator, args: argparse.Namespace):
    match args.command:
        case "register-host":
            return coordinator.register_host(args.host_id, args.owner, args.worker)
        case "report-host":
            return coordinator.report_host(args.host_id, args.worker, _read_json(args.snapshot))
        case "submit":
            return coordinator.submit(
                args.job_id, args.owner, args.profile, args.revision,
                {"os": args.os, "arch": args.arch, "runtime": args.runtime, "memory_mb": args.memory_mb},
                args.restartable,
            )
        case "claim":
            return coordinator.claim(args.host_id, args.worker)
        case "status":
            return coordinator.status(args.job_id)
        case "jobs":
            return coordinator.jobs(args.owner)
        case "hosts":
            return coordinator.hosts()
        case "cancel":
            return coordinator.cancel(args.job_id, args.owner)
        case "pause-guests":
            return coordinator.pause_guests(args.host_id, args.owner, reclaim=args.reclaim)
        case "resume-guests":
            return coordinator.resume_guests(args.host_id, args.owner)
        case "finish":
            return coordinator.finish(args.host, args.worker, args.attempt_id, _read_json(args.result))
    raise ValidationError("unknown command")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "demo":
            value = run_demo()
        elif args.command == "bundle-source":
            value = package_source(args.repo, args.commit, args.output)
        elif args.command.startswith("pilot-"):
            with Pilot(load_config(args.config)) as pilot:
                if args.command == "pilot-submit":
                    value = pilot.submit(
                        args.repo, args.revision, args.job_id, args.owner, args.profile,
                        {"os": args.os, "arch": args.arch, "runtime": args.runtime, "memory_mb": args.memory_mb},
                        args.restartable,
                    )
                elif args.command == "pilot-tick":
                    value = pilot.tick()
                elif args.command == "pilot-result":
                    value = pilot.result(args.job_id)
                else:
                    if args.max_ticks is not None and args.max_ticks < 1:
                        raise ValidationError("max-ticks must be positive")
                    ticks = 0
                    while True:
                        print(json.dumps(pilot.tick(), sort_keys=True, allow_nan=False), flush=True)
                        ticks += 1
                        if args.max_ticks is not None and ticks >= args.max_ticks:
                            return 0
                        time.sleep(pilot.config.poll_seconds)
        else:
            with Coordinator(args.db) as coordinator:
                value = _dispatch(coordinator, args)
        print(json.dumps(value, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except KeyboardInterrupt:
        print(json.dumps({"stopped": "controller_interrupt"}), file=sys.stderr)
        return 130
    except (CoordinatorError, SourceBundleError, TransportError, sqlite3.Error, OSError) as error:
        print(json.dumps({"error": {"code": type(error).__name__, "message": str(error)}}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
