import contextlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import gwf_doctor as doctor


class DoctorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def target(self, name, inputs=(), outputs=()):
        return SimpleNamespace(name=name, inputs=inputs, outputs=outputs,
                               working_dir=str(self.root), options={"memory": "8g"})

    def test_failure_and_transitive_blockers(self):
        targets = [self.target("call", outputs=["calls.json"]),
                   self.target("annotate", ["calls.json"], ["annotation.json"]),
                   self.target("report", ["annotation.json"], ["report.html"])]
        jobs = doctor.parse_accounting("42|OUT_OF_MEMORY|0:9|00:02:00|01:00:00|8Gn|\n42.batch|OUT_OF_MEMORY|0:9|00:02:00|01:00:00|8Gn|7900M\n")
        records = doctor.diagnose(targets, {"call": "42"}, jobs, {})
        self.assertEqual(records[0]["status"], "failed")
        self.assertEqual(records[0]["steps"][0]["MaxRSS"], "7900M")
        self.assertEqual(records[2]["blocked_by"], ["call"])
        self.assertEqual(records[2]["status"], "blocked")

    def test_live_queue_overrides_old_accounting(self):
        jobs = doctor.parse_accounting("42|FAILED|1:0|00:01:00|01:00:00|8Gn|\n")
        queue = doctor.parse_queue("42|RUNNING|None\n")
        record = doctor.diagnose([self.target("call", outputs=["x"])], {"call": "42"}, jobs, queue)[0]
        self.assertEqual(record["status"], "running")

    def test_step_failure_is_not_allocation_state(self):
        jobs = doctor.parse_accounting("42.batch|FAILED|1:0|00:01:00|01:00:00|8Gn|4G\n")
        record = doctor.diagnose([self.target("call")], {"call": "42"}, jobs, {})[0]
        self.assertEqual(record["status"], "unknown")

    def test_completed_missing_output_and_unknown_existing_output(self):
        (self.root / "existing").touch()
        jobs = doctor.parse_accounting("42|COMPLETED|0:0|00:01:00|01:00:00|8Gn|\n")
        records = doctor.diagnose([self.target("a", outputs=["missing"]), self.target("b", outputs=["existing"])], {"a": "42", "b": "43"}, jobs, {})
        self.assertEqual([r["status"] for r in records], ["missing_outputs", "unknown"])

    def test_missing_external_input_and_existing_upstream_output(self):
        (self.root / "calls").touch()
        targets = [self.target("call", outputs=["calls"]), self.target("report", ["calls", "reference"], ["report"])]
        jobs = doctor.parse_accounting("42|FAILED|1:0|00:01:00|01:00:00|8Gn|\n")
        records = doctor.diagnose(targets, {"call": "42"}, jobs, {})
        self.assertEqual(records[1]["blocked_by"], [])
        self.assertEqual(records[1]["missing_external_inputs"], [str(self.root / "reference")])

    def test_cancelled_state_and_malformed_accounting(self):
        jobs = doctor.parse_accounting("42|CANCELLED by 123|0:15|00:01:00|01:00:00|8Gn|\n")
        self.assertEqual(jobs["42"]["allocation"]["State"], "CANCELLED")
        with self.assertRaises(ValueError):
            doctor.parse_accounting("42|FAILED")

    def test_logs_flat_and_hashed_with_bounded_tail(self):
        base = self.root / ".gwf" / "logs"
        base.mkdir(parents=True)
        (base / "call.stderr").write_text("prefix\n" + "a" * 70000 + "\ncommand not found\n")
        logs, warnings = doctor.target_logs(self.root, "call", 1)
        self.assertEqual(logs[0]["tail"], ["command not found"])
        self.assertEqual(warnings, [])
        digest = doctor.hashlib.sha256(b"call").hexdigest()
        hashed = base / digest[0] / digest[1]
        hashed.mkdir(parents=True)
        (hashed / "call.stderr").write_text("new log\n")
        self.assertEqual(doctor.target_logs(self.root, "call", 1)[0][0]["tail"], ["new log"])

    def test_scheduler_failure_is_reported(self):
        args = SimpleNamespace(sacct_file=None, squeue_file=None, offline=False)
        with patch.object(doctor, "run_command", side_effect=FileNotFoundError("not installed")):
            accounting, queue, warnings = doctor.scheduler_snapshot(["42"], args)
        self.assertEqual((accounting, queue), ({}, {}))
        self.assertEqual(len(warnings), 2)

    def test_actual_gwf_cli_and_project_unchanged(self):
        (self.root / "workflow.py").write_text("from gwf import Workflow\ngwf = Workflow()\ngwf.target('call', inputs=[], outputs=['calls.json'], memory='8g') << 'false'\ngwf.target('report', inputs=['calls.json'], outputs=['report.html']) << 'false'\n")
        state = self.root / ".gwf"
        state.mkdir()
        tracked = state / "slurm-backend-tracked.json"
        tracked.write_text(json.dumps({"call": "42"}))
        accounting = self.root / "accounting.txt"
        accounting.write_text("42|OUT_OF_MEMORY|0:9|00:02:00|01:00:00|8Gn|\n")
        before = {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            code = doctor.main([str(self.root), "--offline", "--sacct-file", str(accounting), "--json", "--target", "report", "--tail", "0"])
        self.assertEqual(code, 1)
        report = json.loads(stream.getvalue())
        self.assertEqual(report["targets"][0]["blocked_by"], ["call"])
        after = {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        self.assertFalse((state / "logs").exists())

    def test_fatal_workflow_error_has_exit_two(self):
        (self.root / "workflow.py").write_text("raise RuntimeError('broken workflow')\n")
        stream = io.StringIO()
        with contextlib.redirect_stderr(stream):
            code = doctor.main([str(self.root), "--offline"])
        self.assertEqual(code, 2)
        self.assertIn("broken workflow", stream.getvalue())


if __name__ == "__main__":
    unittest.main()
