import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from sparemesh.metrics import HostSampler, ProcessGroupUsage, _cpu_time, _linux_memory, _mac_memory


MEMORY = {"memory_total_mb": 1000.0, "memory_available_mb": 500.0,
          "memory_pressure": "normal", "swap_used_mb": 0.0}


class HostSamplerTests(unittest.TestCase):
    def test_cpu_delta_first_unknown_and_cached_observation_immutable(self):
        now = [100.0]
        sampler = HostSampler(clock=lambda: now[0], monotonic=lambda: now[0])
        with patch("sparemesh.metrics._cpu_ticks", side_effect=[[100, 80], [200, 150]]) as cpu, \
                patch("sparemesh.metrics._memory", return_value=MEMORY):
            first, errors = sampler.sample("host", guest_eligible=True)
            self.assertIsNone(first["cpu_percent"])
            self.assertEqual(errors, [])
            now[0] = 110
            cached, _ = sampler.sample("host", guest_eligible=False, active_attempt_id="attempt")
            self.assertEqual(first, cached)
            self.assertEqual(cpu.call_count, 1)
            now[0] = 115
            second, _ = sampler.sample("host", guest_eligible=False, active_attempt_id="attempt")
            self.assertAlmostEqual(second["cpu_percent"], 30)
            self.assertNotEqual(first["sample_id"], second["sample_id"])
            self.assertFalse(second["guest_eligible"])
            self.assertEqual(second["active_attempt_id"], "attempt")

    def test_persistent_baseline_and_reboot_reset(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.json"
            with patch("sparemesh.metrics._memory", return_value=MEMORY), \
                    patch("sparemesh.metrics._cpu_ticks", side_effect=[[100, 80], [200, 150], [20, 10]]):
                HostSampler(path, clock=lambda: 100, monotonic=lambda: 100).sample("host")
                second, _ = HostSampler(path, clock=lambda: 115, monotonic=lambda: 115).sample("host")
                self.assertAlmostEqual(second["cpu_percent"], 30)
                reset, _ = HostSampler(path, clock=lambda: 130, monotonic=lambda: 5).sample("host")
                self.assertIsNone(reset["cpu_percent"])
            self.assertEqual(json.loads(path.read_text())["sample"], reset)

    def test_measurement_failure_remains_unknown_with_diagnostics(self):
        with patch("sparemesh.metrics._cpu_ticks", side_effect=OSError), \
                patch("sparemesh.metrics._memory", side_effect=OSError):
            sample, errors = HostSampler().sample("host")
        self.assertIsNone(sample["cpu_percent"])
        self.assertIsNone(sample["memory_available_mb"])
        self.assertIsNone(sample["swap_used_mb"])
        self.assertEqual(sample["memory_pressure"], "unknown")
        self.assertEqual(len(errors), 2)

    def test_linux_memory_is_bytes_and_pressure_is_available_fraction(self):
        text = "MemTotal: 1000000 kB\nMemAvailable: 50000 kB\nSwapTotal: 1000 kB\nSwapFree: 250 kB\n"
        with patch("pathlib.Path.read_text", return_value=text):
            values = _linux_memory()
        self.assertEqual(values["memory_total_mb"], 1024)
        self.assertEqual(values["memory_available_mb"], 51.2)
        self.assertEqual(values["swap_used_mb"], 0.768)
        self.assertEqual(values["memory_pressure"], "critical")

    def test_mac_memory_uses_native_pressure_and_page_size(self):
        sysctl = "hw.memsize: 8000000000\nkern.memorystatus_vm_pressure_level: 2\nvm.swapusage: total = 5.00M used = 1.00M free = 4.00M\n"
        vmstat = ("Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
                  "Pages free: 10.\nPages inactive: 20.\nPages speculative: 30.\n")
        with patch("sparemesh.metrics._command", side_effect=[sysctl, vmstat]):
            values = _mac_memory()
        self.assertEqual(values["memory_total_mb"], 8000)
        self.assertEqual(values["memory_available_mb"], 60 * 16384 / 1_000_000)
        self.assertEqual(values["memory_pressure"], "warning")
        self.assertEqual(values["swap_used_mb"], 1.048576)


class ProcessUsageTests(unittest.TestCase):
    def test_cumulative_sampled_cpu_survives_exit_without_retaining_old_pids(self):
        usage = ProcessGroupUsage(123)
        observations = [[((1, 100), 0.4, 20), ((2, 100), 0.2, 30)],
                        [((1, 100), 0.8, 25)], [((1, 100), 1, 22), ((2, 200), 0.3, 40)]]
        with patch("sparemesh.metrics._group_processes", side_effect=observations):
            for _ in observations:
                usage._sample()
        self.assertAlmostEqual(usage.snapshot()["cpu_seconds"], 1.5)
        self.assertEqual(usage.snapshot()["peak_rss_mb"], 62)
        self.assertEqual(usage.snapshot()["usage_sample_count"], 3)
        self.assertNotIn((2, 100), usage.cpu)

    def test_unobserved_usage_is_unknown(self):
        usage = ProcessGroupUsage(123)
        with patch("sparemesh.metrics._group_processes", side_effect=[[], OSError()]):
            usage._sample()
            usage._sample()
        self.assertEqual(usage.snapshot(), {"cpu_seconds": None, "peak_rss_mb": None, "usage_sample_count": 0})
        self.assertTrue(usage.diagnostics)

    def test_mac_cpu_time_formats(self):
        self.assertEqual(_cpu_time("00:01.23"), 1.23)
        self.assertEqual(_cpu_time("02:03:04.50"), 7384.5)
        self.assertEqual(_cpu_time("1-02:03:04.50"), 93784.5)


if __name__ == "__main__":
    unittest.main()
