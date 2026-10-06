# GWF Doctor

An HPC command-line tool that connects GWF targets to Slurm failure evidence, missing files, dependency blockers, and the latest target logs. Run it in the same Python environment and project directory as your pipeline.

## Install on HPC

Transfer this directory to the cluster, activate your existing GWF environment, then:

```sh
python -m pip install --no-deps /path/to/gwf-doctor
cd /path/to/pipeline
gwf-doctor
```

Tested against the PyPI GWF 1.7.2 release and the upstream GWF source; live Slurm has not yet been tested. Requires Python 3.9+ and GWF 1.7.2 or newer (below 4.0). `--no-deps` preserves your installed GWF version. No administrator privileges or Mac component needed. You can also run `python /path/to/gwf-doctor/gwf_doctor.py` without installing.

```sh
gwf-doctor /path/to/pipeline --all
gwf-doctor --target 'Sample_042*' --tail 20
gwf-doctor --workflow workflow.py:gwf --json > doctor-report.json
```

Target names are used as identifiers; sample identity is not guessed from naming conventions. Target filters retain full dependency analysis.

## What the report means

- `failed`: Slurm reports failure, cancellation, OOM, timeout, or infrastructure termination. Cancellation is shown as its actual Slurm state.
- `missing_outputs`: Slurm reports COMPLETED but declared outputs are absent.
- `blocked`: Missing inputs trace to failed targets, missing-output targets, or targets with missing external inputs. This is a dependency diagnosis, not a prediction of when Slurm will release a job.
- `unknown`: A tracked job has no available allocation state. Output presence does not turn this into success.
- `outputs_present`: All declared outputs exist for an untracked target. This does not establish freshness or biological validity.
- `not_submitted`: No tracked job and outputs absent (or no declared outputs). This alone is not failure.

Active queue states override potentially stale accounting states. Resource fields and step records are included in JSON; MaxRSS is a Slurm per-task/step metric, not necessarily total job memory. Log hints are possible causes, not confirmed diagnoses. Reads are bounded to the last 64 KiB of each log; `--tail 0` omits logs.

Exit codes: 0 = no diagnosed problems; 1 = attention needed; 2 = inspection error or incomplete evidence (takes precedence over 1). JSON goes to stdout; fatal errors go to stderr. Reports can contain sample names, paths, and log content.

## Operational limits

Doctor imports GWF in a temporary directory to contain legacy import-time log-directory creation. Doctor does not submit, cancel, rerun, delete, or modify GWF state. It loads your workflow using GWF, which executes its Python top-level code just like GWF itself; use your own trusted workflow and avoid top-level side effects. It does not call `gwf status`, because initializing GWF backends can migrate logs or rewrite tracking state.

This is a snapshot, not a monitor. Scheduler and filesystem state can change while inspecting. GWF tracking and logs normally represent the latest attempt, not an archive. Historical jobs removed from tracking cannot be diagnosed. Doctor does not reproduce GWF spec hashes/timestamp freshness decisions or validate genomic outputs.

Current adapters read `.gwf/slurm-backend-tracked.json` and hashed or flat `.gwf/logs` layouts. These formats are implementation details; unsupported tracking identifiers fail explicitly. Test your installed GWF version before relying on reports.

## Offline inspection and tests

Save scheduler evidence on HPC:

```sh
sacct --noheader --parsable2 --jobs JOB_IDS --format=JobIDRaw,State,ExitCode,Elapsed,Timelimit,ReqMem,MaxRSS > accounting.txt
squeue --noheader --jobs JOB_IDS '--format=%i|%T|%r' > queue.txt
gwf-doctor --offline --sacct-file accounting.txt --squeue-file queue.txt
python -m unittest discover -s tests -v
```

The tests use synthetic workflows and scheduler records. Live cluster behavior still needs verification on your HPC.
# gwf-doctor
