"""Best-effort host observations and sampled process-group resource estimates.

Only counters and aggregate measurements leave this module. Process command lines,
names, and per-process history are never persisted. MB uses decimal units.
"""

from contextlib import contextmanager
import ctypes
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid


MB = 1_000_000


def _command(argv):
    return subprocess.run(argv, capture_output=True, check=True, timeout=2,
                          env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C"}).stdout.decode("ascii")


def _cpu_ticks():
    if sys.platform == "linux":
        values = Path("/proc/stat").read_text().splitlines()[0].split()
        if values[0] != "cpu" or len(values) < 5:
            raise ValueError("missing CPU counters")
        # Guest counters are already included in user/nice. I/O wait is idle.
        ticks = [int(value) for value in values[1:9]]
        return [sum(ticks), ticks[3] + (ticks[4] if len(ticks) > 4 else 0)]
    if sys.platform == "darwin":
        library = ctypes.CDLL(None)
        library.mach_host_self.restype = ctypes.c_uint
        library.host_statistics.argtypes = [ctypes.c_uint, ctypes.c_int,
                                            ctypes.POINTER(ctypes.c_uint), ctypes.POINTER(ctypes.c_uint)]
        library.host_statistics.restype = ctypes.c_int
        counters = (ctypes.c_uint * 4)()
        count = ctypes.c_uint(4)
        # HOST_CPU_LOAD_INFO is user, system, idle, nice, in scheduler ticks.
        host = library.mach_host_self()
        try:
            if library.host_statistics(host, 3, counters, ctypes.byref(count)) or count.value != 4:
                raise OSError("host CPU counters unavailable")
        finally:
            library.mach_task_self.restype = ctypes.c_uint
            library.mach_port_deallocate.argtypes = [ctypes.c_uint, ctypes.c_uint]
            library.mach_port_deallocate(library.mach_task_self(), host)
        return [sum(counters), counters[2]]
    raise OSError("host metrics require Linux or macOS")


def _linux_memory():
    values = {key: int(value) * 1024 for key, value in
              re.findall(r"^(\w+):\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.M)}
    total = values.get("MemTotal")
    available = values.get("MemAvailable")
    pressure = "unknown"
    if total and available is not None:
        fraction = available / total
        pressure = "critical" if fraction <= 0.05 else "warning" if fraction <= 0.10 else "normal"
    swap = None
    if "SwapTotal" in values and "SwapFree" in values:
        swap = max(0, values["SwapTotal"] - values["SwapFree"]) / MB
    return {"memory_total_mb": total / MB if total is not None else None,
            "memory_available_mb": available / MB if available is not None else None,
            "memory_pressure": pressure, "swap_used_mb": swap}


def _mac_memory():
    output = _command(["/usr/sbin/sysctl", "hw.memsize", "kern.memorystatus_vm_pressure_level", "vm.swapusage"])
    total = re.search(r"^hw.memsize:\s+(\d+)$", output, re.M)
    pressure = re.search(r"^kern.memorystatus_vm_pressure_level:\s+(\d+)$", output, re.M)
    swap = re.search(r"used\s*=\s*([\d.]+)([KMG])", output)
    pages = _command(["/usr/bin/vm_stat"])
    page_size = re.search(r"page size of (\d+) bytes", pages)
    available = None
    if page_size:
        counts = [re.search(rf"^{label}:\s+(\d+)\.", pages, re.M)
                  for label in ("Pages free", "Pages inactive", "Pages speculative")]
        if all(counts):
            available = sum(int(value.group(1)) for value in counts) * int(page_size.group(1)) / MB
    return {"memory_total_mb": int(total.group(1)) / MB if total else None,
            "memory_available_mb": available,
            "memory_pressure": {1: "normal", 2: "warning", 4: "critical"}.get(
                int(pressure.group(1)) if pressure else None, "unknown"),
            "swap_used_mb": float(swap.group(1)) * {"K": 1024, "M": 1024**2, "G": 1024**3}[swap.group(2)] / MB if swap else None}


def _memory():
    if sys.platform == "linux":
        return _linux_memory()
    if sys.platform == "darwin":
        return _mac_memory()
    raise OSError("host metrics require Linux or macOS")


def _write_json(path, value):
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".metrics-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, allow_nan=False)
            stream.flush()
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


class HostSampler:
    """Cache one observation across short-lived worker RPC processes."""

    def __init__(self, state_path=None, *, clock=time.time, monotonic=time.monotonic):
        self.state_path = Path(state_path) if state_path else None
        self.clock = clock
        self.monotonic = monotonic
        self.state = {}
        self.lock = threading.Lock()

    @contextmanager
    def _locked(self):
        with self.lock:
            if self.state_path is None:
                yield
            else:
                with self.state_path.with_suffix(".lock").open("a+b") as stream:
                    fcntl.flock(stream, fcntl.LOCK_EX)
                    yield

    def sample(self, host_id, *, guest_eligible=None, active_attempt_id=None, min_interval=15):
        with self._locked():
            diagnostics = []
            state = self.state
            if self.state_path and self.state_path.exists():
                try:
                    state = json.loads(self.state_path.read_text())
                    if not isinstance(state, dict):
                        raise ValueError("invalid sampler state")
                except (OSError, ValueError):
                    state = {}
                    diagnostics.append("host CPU baseline could not be read")
            now = self.clock()
            cached = state.get("sample")
            if (isinstance(cached, dict) and cached.get("host_id") == host_id
                    and isinstance(cached.get("sampled_at"), (int, float))
                    and 0 <= now - cached["sampled_at"] < min_interval):
                return cached, state.get("diagnostics", [])
            sample = {"schema_version": 1, "sample_id": str(uuid.uuid4()), "host_id": host_id,
                      "sampled_at": now, "cpu_percent": None, "memory_total_mb": None,
                      "memory_available_mb": None, "memory_pressure": "unknown", "swap_used_mb": None,
                      "guest_eligible": guest_eligible, "active_attempt_id": active_attempt_id}
            baseline = None
            try:
                ticks = _cpu_ticks()
                monotonic = self.monotonic()
                previous = state.get("baseline")
                if (isinstance(previous, dict) and previous.get("platform") == sys.platform
                        and monotonic > previous.get("monotonic", monotonic)):
                    old = previous.get("ticks", [])
                    if len(old) == 2 and all(current >= before for current, before in zip(ticks, old)):
                        total, idle = (current - before for current, before in zip(ticks, old))
                        if total > 0 and 0 <= idle <= total:
                            sample["cpu_percent"] = 100 * (total - idle) / total
                baseline = {"platform": sys.platform, "ticks": ticks, "monotonic": monotonic}
            except (OSError, ValueError, TypeError, AttributeError, IndexError):
                diagnostics.append("host CPU measurement unavailable")
            try:
                sample.update(_memory())
            except (OSError, ValueError, subprocess.SubprocessError, UnicodeError):
                diagnostics.append("host memory measurement unavailable")
            self.state = {"baseline": baseline, "sample": sample, "diagnostics": diagnostics}
            if self.state_path:
                try:
                    _write_json(self.state_path, self.state)
                except OSError:
                    diagnostics.append("host sample cache could not be written")
            return sample, diagnostics


def _cpu_time(value):
    days, separator, value = value.rpartition("-")
    fields = value.split(":")
    if len(fields) not in (2, 3):
        raise ValueError("invalid process CPU time")
    seconds = float(fields[-1]) + 60 * int(fields[-2])
    if len(fields) == 3:
        seconds += 3600 * int(fields[0])
    return seconds + (86400 * int(days) if separator else 0)


def _group_processes(group):
    """Return ephemeral (identity, CPU seconds, RSS MB) tuples for this group."""
    processes = []
    if sys.platform == "linux":
        ticks_per_second = os.sysconf("SC_CLK_TCK")
        page_size = os.sysconf("SC_PAGE_SIZE")
        for path in Path("/proc").glob("[0-9]*/stat"):
            try:
                text = path.read_text()
                fields = text[text.rindex(")") + 2:].split()
                if int(fields[2]) == group:
                    processes.append(((int(path.parent.name), int(fields[19])),
                                      (int(fields[11]) + int(fields[12])) / ticks_per_second,
                                      max(0, int(fields[21])) * page_size / MB))
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                continue
        return processes
    if sys.platform == "darwin":
        # lstart distinguishes PID reuse. No process names or command lines are requested.
        for line in _command(["/bin/ps", "-axo", "pid=,pgid=,time=,rss=,lstart="]).splitlines():
            fields = line.split(maxsplit=4)
            if len(fields) == 5 and int(fields[1]) == group:
                processes.append(((int(fields[0]), fields[4]), _cpu_time(fields[2]), int(fields[3]) * 1024 / MB))
        return processes
    raise OSError("process metrics require Linux or macOS")


class ProcessGroupUsage:
    """Sample a group's aggregate usage without delaying its supervisor loop.

    CPU time is the sum of each observed process's largest lifetime counter.
    Processes that start and exit between samples can be missed. RSS is the
    largest observed sum and can count shared pages more than once.
    """

    def __init__(self, group, *, interval=1):
        self.group = group
        self.interval = interval
        self.cpu = {}
        self.completed_cpu_seconds = 0.0
        self.peak_rss_mb = None
        self.sample_count = 0
        self.diagnostics = []
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def _sample(self):
        try:
            processes = _group_processes(self.group)
            if not processes:
                return
            with self.lock:
                observed = {identity for identity, _, _ in processes}
                self.completed_cpu_seconds += sum(seconds for identity, seconds in self.cpu.items()
                                                  if identity not in observed)
                previous = self.cpu
                self.cpu = {}
                for identity, seconds, _ in processes:
                    self.cpu[identity] = max(previous.get(identity, 0), seconds)
                rss = sum(rss for _, _, rss in processes)
                self.peak_rss_mb = max(self.peak_rss_mb or 0, rss)
                self.sample_count += 1
        except Exception:
            with self.lock:
                if not self.diagnostics:
                    self.diagnostics.append("process-group resource measurement unavailable")

    def _run(self):
        while not self.stopped.is_set():
            self._sample()
            self.stopped.wait(self.interval)

    def snapshot(self):
        with self.lock:
            return {"cpu_seconds": self.completed_cpu_seconds + sum(self.cpu.values()) if self.sample_count else None,
                    "peak_rss_mb": self.peak_rss_mb, "usage_sample_count": self.sample_count}

    def stop(self):
        self.stopped.set()
        self.thread.join(timeout=2.5)
        return self.snapshot()
