"""Evidence-based diagnostics for a GWF workflow running on Slurm."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import fnmatch
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

FIELDS = ("JobIDRaw", "State", "ExitCode", "Elapsed", "Timelimit", "ReqMem", "MaxRSS")
FAILURES = {"FAILED", "OUT_OF_MEMORY", "TIMEOUT", "NODE_FAIL", "BOOT_FAIL",
            "PREEMPTED", "DEADLINE", "CANCELLED", "REVOKED"}
ACTIVE = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED",
          "REQUEUED", "REQUEUE_HOLD", "RESIZING", "STAGE_OUT"}
HINTS = (
    (r"out.of.memory|oom.kill|oom_kill|cannot allocate memory", "Possible memory exhaustion"),
    (r"disk quota exceeded|no space left on device", "Possible storage/quota exhaustion"),
    (r"command not found|ModuleNotFoundError|ImportError", "Possible software/environment problem"),
    (r"permission denied", "Possible permissions problem"),
    (r"no such file or directory|FileNotFoundError", "Possible missing file"),
)


def state_name(value):
    return value.strip().split()[0].rstrip("+") if value.strip() else "UNKNOWN"


def parse_accounting(text):
    jobs = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < len(FIELDS):
            raise ValueError("Invalid sacct row; expected: " + "|".join(FIELDS))
        row = dict(zip(FIELDS, parts))
        if row["JobIDRaw"] == "JobIDRaw":
            continue
        job = row["JobIDRaw"].split(".")[0]
        record = jobs.setdefault(job, {"allocation": None, "steps": []})
        row["State"] = state_name(row["State"])
        if "." in row["JobIDRaw"]:
            record["steps"].append(row)
        else:
            record["allocation"] = row
    return jobs


def parse_queue(text):
    result = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("|", 2)
        if len(parts) != 3:
            raise ValueError("Invalid squeue row; expected JobID|State|Reason")
        job, state, reason = parts
        result[job] = {"state": state_name(state), "reason": reason.strip()}
    return result


def run_command(args):
    proc = subprocess.run(args, capture_output=True, text=True, timeout=30)
    if proc.returncode:
        raise RuntimeError(proc.stderr.strip() or "command returned " + str(proc.returncode))
    return proc.stdout


def scheduler_snapshot(job_ids, args):
    warnings, accounting, queue = [], {}, {}
    if args.sacct_file:
        accounting = parse_accounting(Path(args.sacct_file).read_text())
    elif job_ids and not args.offline:
        for offset in range(0, len(job_ids), 200):
            batch = job_ids[offset:offset + 200]
            try:
                accounting.update(parse_accounting(run_command([
                    "sacct", "--noheader", "--parsable2", "--jobs", ",".join(batch),
                    "--format=" + ",".join(FIELDS)])))
            except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as exc:
                warnings.append("sacct unavailable: " + str(exc))
    if args.squeue_file:
        queue = parse_queue(Path(args.squeue_file).read_text())
    elif job_ids and not args.offline:
        for offset in range(0, len(job_ids), 200):
            batch = job_ids[offset:offset + 200]
            try:
                queue.update(parse_queue(run_command([
                    "squeue", "--noheader", "--jobs", ",".join(batch),
                    "--format=%i|%T|%r"])))
            except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as exc:
                warnings.append("squeue unavailable: " + str(exc))
    return accounting, queue, list(dict.fromkeys(warnings))


def flatten_paths(value):
    if isinstance(value, (str, os.PathLike)):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from flatten_paths(child)
    else:
        for child in value:
            yield from flatten_paths(child)


def target_paths(target, attribute):
    base = Path(target.working_dir)
    return [os.path.abspath(base / Path(p)) for p in flatten_paths(getattr(target, attribute))]


def read_tail(path, lines):
    # Cap reads even when a log is many gigabytes long.
    with path.open("rb") as stream:
        stream.seek(0, 2)
        size = stream.tell()
        start = max(0, size - 65536)
        stream.seek(start)
        data = stream.read()
    if start:
        data = data.partition(b"\n")[2]
    return data.decode("utf-8", errors="replace").splitlines()[-lines:]


def target_logs(project, name, lines):
    digest = hashlib.sha256(name.encode()).hexdigest()
    bases = [project / ".gwf" / "logs" / digest[0] / digest[1] / name,
             project / ".gwf" / "logs" / name]
    logs, warnings = [], []
    for suffix in ("stderr", "stdout"):
        for index, base in enumerate(bases):
            path = base.with_suffix("." + suffix) if index == 0 else Path(str(base) + "." + suffix)
            if path.is_file():
                try:
                    logs.append({"stream": suffix, "path": str(path),
                                 "tail": read_tail(path, lines)})
                except OSError as exc:
                    warnings.append("Cannot read log " + str(path) + ": " + str(exc))
                break
    return logs, warnings


def diagnose(targets, tracked, accounting, queue):
    records, producers = {}, {}
    for target in targets:
        for output in target_paths(target, "outputs"):
            if output in producers:
                raise ValueError("Output produced by multiple targets: " + output)
            producers[output] = target.name
    for target in targets:
        name = target.name
        job_id = str(tracked[name]) if name in tracked else None
        job = accounting.get(job_id, {})
        allocation = job.get("allocation")
        state = allocation["State"] if allocation else "UNKNOWN"
        live = queue.get(job_id)
        if live:
            state = live["state"]
        inputs = target_paths(target, "inputs")
        outputs = target_paths(target, "outputs")
        missing_inputs = [p for p in inputs if not Path(p).exists()]
        missing_outputs = [p for p in outputs if not Path(p).exists()]
        if state in ACTIVE:
            status = state.lower()
        elif state in FAILURES:
            status = "failed"
        elif state == "COMPLETED":
            status = "missing_outputs" if missing_outputs else "completed"
        elif job_id:
            status = "unknown"
        else:
            status = "outputs_present" if outputs and not missing_outputs else "not_submitted"
        dependencies = sorted({producers[p] for p in inputs if p in producers})
        records[name] = {
            "name": name, "status": status, "job_id": job_id, "slurm_state": state,
            "queue_reason": live["reason"] if live else None,
            "allocation": allocation, "steps": job.get("steps", []),
            "requested_resources": dict(target.options),
            "missing_inputs": missing_inputs, "missing_outputs": missing_outputs,
            "missing_external_inputs": [p for p in missing_inputs if p not in producers],
            "dependencies": dependencies, "blocked_by": [],
        }
    # Follow only missing input edges: an existing upstream output can be usable
    # even if its producer's latest attempt failed.
    visiting, done = set(), set()
    def blockers(name):
        if name in visiting:
            raise ValueError("Cycle in workflow dependencies at " + name)
        if name in done:
            return records[name]["blocked_by"]
        visiting.add(name)
        record = records[name]
        result = set()
        for path in record["missing_inputs"]:
            dependency = producers.get(path)
            if dependency:
                upstream = records[dependency]
                roots = blockers(dependency)
                if roots:
                    result.update(roots)
                elif upstream["status"] in {"failed", "missing_outputs"} or upstream["missing_external_inputs"]:
                    result.add(dependency)
        record["blocked_by"] = sorted(result)
        if result and record["status"] in {"not_submitted", "unknown", "pending"}:
            record["status"] = "blocked"
        visiting.remove(name)
        done.add(name)
        return record["blocked_by"]
    for name in records:
        blockers(name)
    return list(records.values())


def render(report, show_all):
    print("GWF Doctor — " + report["project"])
    print("Summary: " + ", ".join(f"{count} {state}" for state, count in sorted(report["summary"].items())))
    for warning in report["warnings"]:
        print("Warning: " + warning)
    for item in report["targets"]:
        if not show_all and not item["attention"]:
            continue
        print(f"\n{item['name']}: {item['status']} (job {item['job_id'] or 'none'})")
        if item["job_id"]:
            print("  Slurm: " + item["slurm_state"])
        if item["allocation"]:
            row = item["allocation"]
            print(f"  Exit: {row['ExitCode']}; elapsed: {row['Elapsed']}; limit: {row['Timelimit']}; requested memory: {row['ReqMem']}")
        if item["queue_reason"]:
            print("  Queue reason: " + item["queue_reason"])
        if item["blocked_by"]:
            print("  Blocked by: " + ", ".join(item["blocked_by"]))
        for label, key in (("Missing external input", "missing_external_inputs"), ("Missing output", "missing_outputs")):
            for path in item[key]:
                print("  " + label + ": " + path)
        for hint in item["hints"]:
            print("  Log hint (latest target log; may be from an earlier attempt): " + hint)
        for log in item["logs"]:
            print("  " + log["stream"] + ": " + log["path"])
            for line in log["tail"]:
                print("    " + line)
    if not any(t["attention"] for t in report["targets"]):
        print("No diagnosed failures or missing external inputs.")
    print("\nOutput presence is not a GWF freshness check. Logs describe the latest stored attempt.")



class GraphFilesystem:
    """Defer input existence checks to Doctor while GWF validates structure."""

    def exists(self, path):
        return True


def validate_graph(graph_class, targets):
    # GWF 2.x requires fs and rejects missing external inputs at graph creation.
    # Doctor must keep those workflows inspectable, then report missing inputs.
    parameters = inspect.signature(graph_class.from_targets).parameters
    if "fs" in parameters:
        return graph_class.from_targets(targets, fs=GraphFilesystem())
    return graph_class.from_targets(targets)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project", nargs="?", default=".", help="Pipeline project directory")
    parser.add_argument("--workflow", default="workflow.py", help="Workflow filename, optionally filename:object")
    parser.add_argument("--target", action="append", default=[], help="Target glob; repeat to select multiple patterns")
    parser.add_argument("--all", action="store_true", help="Include healthy and active targets in terminal output")
    parser.add_argument("--json", action="store_true", help="Emit a machine-readable report")
    parser.add_argument("--tail", type=int, default=8, help="Log lines per stream (0 to disable)")
    parser.add_argument("--offline", action="store_true", help="Do not call Slurm")
    parser.add_argument("--sacct-file", help="Read saved headerless pipe-separated accounting output")
    parser.add_argument("--squeue-file", help="Read saved JobID|State|Reason output")
    args = parser.parse_args(argv)
    try:
        if not 0 <= args.tail <= 1000:
            raise ValueError("--tail must be between 0 and 1000")
        project = Path(args.project).resolve()
        if not project.is_dir():
            raise ValueError("Project directory does not exist: " + str(project))
        # Preserve fixture paths before entering the workflow directory.
        for key in ("sacct_file", "squeue_file"):
            if getattr(args, key):
                setattr(args, key, str(Path(getattr(args, key)).resolve()))
        previous_dir = Path.cwd()
        # GWF 1.7.2 creates .gwf/logs at import time. Contain that side effect.
        with tempfile.TemporaryDirectory(prefix="gwf-doctor-import-") as temp:
            try:
                os.chdir(temp)
                from gwf import Workflow
                from gwf.core import Graph
            finally:
                os.chdir(previous_dir)
        workflow_file = args.workflow.split(":", 1)[0]
        if not (project / workflow_file).is_file():
            raise ValueError("Workflow file does not exist in project: " + workflow_file)
        previous_bytecode = sys.dont_write_bytecode
        try:
            sys.dont_write_bytecode = True
            os.chdir(project)
            workflow = Workflow.from_path(args.workflow)
        finally:
            sys.dont_write_bytecode = previous_bytecode
            os.chdir(previous_dir)
        validate_graph(Graph, workflow.targets)
        state_path = project / ".gwf" / "slurm-backend-tracked.json"
        tracked = json.loads(state_path.read_text()) if state_path.exists() else {}
        if not isinstance(tracked, dict) or any(not re.fullmatch(r"\d+(?:_\d+)?", str(v)) for v in tracked.values()):
            raise ValueError("Unsupported GWF Slurm tracking format in " + str(state_path))
        ids = sorted({str(v) for k, v in tracked.items() if k in workflow.targets})
        accounting, queue, warnings = scheduler_snapshot(ids, args)
        records = diagnose(list(workflow.targets.values()), tracked, accounting, queue)
        if args.target:
            records = [r for r in records if any(fnmatch.fnmatchcase(r["name"], p) for p in args.target)]
            if not records:
                raise ValueError("No targets match --target")
        for item in records:
            item["attention"] = item["status"] in {"failed", "missing_outputs", "blocked", "unknown"} or bool(item["missing_external_inputs"])
            item["logs"], item["hints"] = [], []
            if args.tail and (item["attention"] or args.all):
                item["logs"], log_warnings = target_logs(project, item["name"], args.tail)
                warnings.extend(log_warnings)
                text = "\n".join(line for log in item["logs"] for line in log["tail"])
                item["hints"] = [label for pattern, label in HINTS if re.search(pattern, text, re.I)]
        unknown = [r["name"] for r in records if r["job_id"] and r["slurm_state"] == "UNKNOWN"]
        if unknown:
            warnings.append("No scheduler state available for " + str(len(unknown)) + " tracked targets; completion cannot be established.")
        report = {"schema_version": 1, "project": str(project),
                  "observed_at": datetime.now(timezone.utc).isoformat(),
                  "summary": dict(Counter(r["status"] for r in records)),
                  "warnings": warnings, "targets": records}
        if args.json:
            print(json.dumps(report, indent=2, default=str))
        else:
            render(report, args.all)
        if warnings:
            return 2
        return 1 if any(r["attention"] for r in records) else 0
    except Exception as exc:
        print("gwf-doctor: " + str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
