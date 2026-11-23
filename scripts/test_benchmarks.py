import copy
import json
from pathlib import Path
import unittest

from benchmark import percentile, workload
from summarize_benchmarks import comparable, summarize

ROOT = Path(__file__).resolve().parents[1]


class BenchmarkTests(unittest.TestCase):
    def test_percentile_is_nearest_rank(self):
        self.assertEqual(percentile(list(range(1, 21))), 19)

    def test_mixed_workload_keeps_even_composition(self):
        self.assertEqual([job["type"] for job in workload("mixed", 4)],
                         ["sleep", "compute", "sleep", "compute"])

    def test_wall_clock_gap_excludes_timing_but_preserves_records(self):
        self.assertFalse(comparable({"total_seconds": 12, "observed_seconds": 6}))
        self.assertTrue(comparable({"total_seconds": 12, "observed_seconds": 12.05}))

    def test_saved_snapshot_matches_validated_raw_data(self):
        report = json.loads((ROOT / "benchmarks/results.json").read_text())
        saved = json.loads((ROOT / "frontend/public/benchmarks.json").read_text())
        self.assertEqual(summarize(report), saved)
        invalid = copy.deepcopy(report)
        invalid["trials"][1]["jobs"][0]["id"] = invalid["trials"][0]["jobs"][0]["id"]
        with self.assertRaisesRegex(ValueError, "distinct job IDs"):
            summarize(invalid)


if __name__ == "__main__":
    unittest.main()
