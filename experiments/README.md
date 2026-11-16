# OpenStack Swift ring-builder and composite-builder save experiments

These experiments test the **actual OpenStack Swift fork in `EC528_swift`**.
They cover two changes to upstream Swift:

- **Ring builder (experiments 0–4).** The fix changes `RingBuilder.save()` in
  `swift/common/ring/builder.py`: it converts the destination to an absolute
  path, writes and syncs a `NamedTemporaryFile` beside it, and replaces the
  existing builder with `renamer()`, which syncs the containing directory. All
  of these steps run inside one `try` block, so a failure at any of them
  removes the temporary file and re-raises the error.
- **Composite ring builder (experiments 5–11).** The fix changes
  `CompositeRingBuilder.save()` in `swift/common/ring/composite_builder.py` in
  the same way. See [The composite-builder change](#the-composite-builder-change)
  below.

The tests below provide distinct evidence for each change.

### Ring builder

| Experiment | Claim or evidence supported | Checks |
| --- | --- | --- |
| [0: unit suite](exp0_unittests.py) | Baseline and regression evidence for upstream Swift | Runs existing unit tests and reports failures, errors, skips, and elapsed time. |
| [1: partial serialization](exp1_run_partial_dump_test.py) | An interrupted serialization should preserve the previous builder | Runs the existing partial-pickle regression test. |
| [2: process interruption](exp2_interrupted_save.py) | Killing a writer leaves the complete old or new builder, depending on whether replacement occurred | Three controlled `SIGKILL` scenarios; reload, placement, and subsequent-save checks. |
| [3: handled failures](exp3_save_failure_cleanup.py) | Handled pre-replacement errors preserve the old builder, propagate the error, and clean up the attempt | Four injected failure points, plus failed-first-save ID recovery. |
| [4: successful-save protocol](exp4_save_durability_order.py) | A successful update follows the temporary-file durability sequence | Observes real serialization, flush, sync, close, and rename operations and validates the installed state. |

### Composite ring builder

| Experiment | Claim or evidence supported | Checks |
| --- | --- | --- |
| [5: composite unit suite](exp5_composite_unittests.py) | Regression evidence for upstream's composite builder tests | Runs `test_composite_builder.py` and reports failures, errors, skips, and elapsed time. |
| [6: partial serialization](exp6_composite_partial_dump.py) | An interrupted serialization preserves the previous composite file | Half-written `json.dump` then an error; byte, load, placement, and temporary-file checks. |
| [7: process interruption](exp7_composite_interrupted_save.py) | Killing a writer leaves the complete old or new composite file | Four controlled `SIGKILL` scenarios; reload, placement, and subsequent-save checks. |
| [8: handled failures](exp8_composite_save_failure_cleanup.py) | Handled errors preserve the old file, propagate the error, and clean up the attempt | Four injected failure points, failed first save, failing cleanup, uncomposed save, and retry. |
| [9: successful-save protocol](exp9_composite_save_durability_order.py) | A successful update follows the temporary-file durability sequence | Observes real serialization, flush, sync, close, and rename operations and validates the installed state. |
| [10: paths, repetition, readers](exp10_composite_save_edge_cases.py) | Saves work for relative and new paths, repeat cleanly, and never expose a partial file to a reader | Relative path, new path, 25 repeated saves, a concurrent reader process, and the persisted format. |
| [11: file permissions](exp11_composite_save_file_mode.py) | Saves never make the composite file more permissive | Five starting modes for an update, plus a first save, against the umask-derived mode. |

## The composite-builder change

Compared with upstream Swift, `swift/common/ring/composite_builder.py` changes
in two places. Nothing else in the module, and no other composite-builder
behavior, is changed.

**Imports.** Two imports are added:

```python
from tempfile import NamedTemporaryFile
from swift.common.utils import renamer
```

**`CompositeRingBuilder.save()`.** Upstream opened the live file with
`open(path_to_file, 'wt')`, which truncates it immediately, then built the
metadata and streamed `json.dump` into it, with no `fsync`. A crash, full disk,
or exception part-way through left an empty or truncated file, and the next
`CompositeRingBuilder.load()` failed with *"File does not contain valid
composite ring data"*. A reader running during a save could see the same
broken state without any crash.

The patched method:

1. Keeps the existing "No composed ring to save" guard, which runs before any
   file is touched.
2. Converts `path_to_file` to an absolute path. Swift's `renamer()` creates the
   destination's directory and fails on the empty directory name of a bare file
   name such as `comp.json`; upstream's `test_abs_paths_persisted` saves exactly
   that way.
3. Builds the metadata (`to_dict()`, `saved_path`, `serialization_version`)
   before any file is opened. The persisted format is unchanged.
4. Writes the JSON to a `NamedTemporaryFile` in the destination's directory,
   named with the destination's name as a prefix and `delete=False`.
5. Calls `flush()` and `os.fsync()` on the temporary file, then closes it.
6. Replaces the destination with `renamer(..., fsync=True)`: an atomic
   `os.rename` followed by a sync of the containing directory.
7. On any exception, removes the temporary file (ignoring an `OSError` from the
   removal itself) and re-raises the original error.

Readers therefore see the complete previous file or the complete new one,
never a partial file. One side effect: `NamedTemporaryFile` creates files with
mode `0600`, so a saved composite file is owner-only even if the previous file
was, say, `0644`. Experiment 11 accepts this narrowing but rejects any save that
adds permissions.

## Setup and running

Use the prepared Linux environment. From the fork root:

```bash
cd /path/to/EC528_swift
source .venv/bin/activate
python3 experiments/exp2_interrupted_save.py
echo "exit=$?"
```

On the team's `SwiftBugFix` Lightsail instance, connect using SSH and use the
existing checkout path:

```bash
cd /home/ubuntu/EC528_swift
source .venv/bin/activate
python3 experiments/exp2_interrupted_save.py
python3 experiments/exp3_save_failure_cleanup.py
python3 experiments/exp4_save_durability_order.py
```

The composite experiments run the same way:

```bash
cd /home/ubuntu/EC528_swift
source .venv/bin/activate
for f in experiments/exp{5,6,7,8,9,10,11}_composite_*.py; do
    python3 "$f"; echo "$f exit=$?"
done
```

If the checkout is at `/opt/ec528-swift`, as in experiment 0's opening example,
use that directory instead. Verify the checkout contains all new scripts,
`atomic_save_support.py` (experiments 2–4), and `composite_save_support.py`
(experiments 6–11). Run each separately with `echo "exit=$?"` when
recording an individual result; the shell does not stop the sequence above
just because an earlier experiment fails.

The existing `setup.sh` prepares dependencies and scratch storage on a clean
Linux machine. It changes system packages and mounts; it is not an experiment.
No running Swift cluster or production builder file is needed for experiments
2–4 or 6–11: they create populated builders and composite files in temporary
directories.

## Experiment 0: upstream regression baseline

**Claim supported:** comparing the upstream unit suite before and after the
patch provides evidence that the change has not introduced new test failures.
A single run alone does not establish a before/after comparison.

```bash
python3 experiments/exp0_unittests.py \
    --results-json baseline.json --timeout 1800
```

The runner discovers a Swift checkout, checks the environment, and runs its unit
suite. Record the revision and named failures on both versions. `--quick`
restricts execution to the ring tests and is only a smoke test, not the full
baseline. Runtime is bounded by the requested timeout (30 minutes by default).

**Limits:** unit-suite results do not establish complete system correctness or
measure the patch's performance cost. Inspect the pytest output as well as the
final label.

## Experiment 1: partial-serialization regression

**Claim tested:** if serialization writes only part of a builder and then raises
an error, the previous builder file should remain byte-identical and loadable.

```bash
python3 experiments/exp1_run_partial_dump_test.py
```

Runs exactly
`TestRingBuilder.test_save_partial_dump_does_nothing` from
`test/unit/common/ring/test_builder.py`. The test first saves a valid builder,
then patches `pickle.dump` to write half its payload and raise `OSError`. It
checks the saved bytes and loads the builder again. This normally takes seconds.

**Expected comparison:** direct overwriting should fail preservation; an atomic
save preserves the file. If the test does fail, the runner's diagnosis reports
whether the file bytes changed.

**Limits:** the failure is injected, not an actual killed process. It does not
verify syncing, concurrent writers, composite builders, or power-loss survival.

## Experiment 2: process interruption before and after replacement

**Claim tested:** until replacement, an interrupted save preserves the previous
valid builder. After replacement, a process crash leaves the complete new
builder available to the next reader.

```bash
python3 experiments/exp2_interrupted_save.py
```

Three tests run saves of a changed builder in child processes. Each child pauses
at a controlled point and is killed by the parent with `SIGKILL`:

1. Halfway through serialization: the original bytes and loaded state survive.
2. Immediately before rename: the original bytes and loaded state survive.
3. Immediately after rename: the complete updated state loads successfully.

The checks also compare individual partition-to-device lookups. For kills
before replacement, a later save must work despite any temporary file left
behind. A passing run reports **3 tests**, `OK`, and `RESULT: PASS`.

**Limits:** a hard kill cannot run exception cleanup, so leftover temporary
files are permitted. The OS continues running; the post-rename test does not
establish durability after power loss before the directory sync. These are
individual-builder lookups, not composite-ring CLI lookups.

## Experiment 3: handled failure preservation and cleanup

**Claim tested:** errors handled before replacement leave the previous builder
unchanged and loadable, propagate to the caller, and remove the temporary file
created by that attempt. A failed first save resets its unpersisted builder ID.

```bash
python3 experiments/exp3_save_failure_cleanup.py
```

Five tests cover:

1. Serialization writing half the payload and raising an error.
2. The explicit file flush raising an error.
3. File `fsync` raising an error.
4. Replacement raising an error, including Swift's rename retry.
5. A first save failing before it can persist its newly assigned ID.

Each injected failure must actually be reached and raise the expected error.
Existing-builder checks compare the original bytes, loaded state, and
partition-device lookups, and require no extra files after cleanup. The
first-save check requires no destination or temporary file and a restored
unpersisted ID. A passing run reports **5 tests**, `OK`, and `RESULT: PASS`.

**Limits:** this is caught-exception cleanup on a writable filesystem. It does
not require cleanup after `SIGKILL`, guarantee that unlink can never fail, or
require rollback after replacement has already occurred.

## Experiment 4: successful-save durability ordering

**Claim tested:** a changed builder is prepared completely before replacement,
and a successful save follows the required file and directory sync sequence.

```bash
python3 experiments/exp4_save_durability_order.py
```

The single test observes real operations while saving a changed builder:

```text
serialize -> flush -> file_fsync -> close -> rename -> directory_fsync
```

It checks that the candidate is a separate file in the destination directory;
the original stays unchanged until replacement; the candidate is complete and
closed before rename; and the synced directory descriptor identifies the actual
parent directory. The resulting builder must match the intended new state,
load successfully, retain its partition-device mappings, and leave no file at
the temporary name. A passing run reports **1 test**, `OK`, and `RESULT: PASS`.

**Limits:** observing successful sync calls is not a machine power-loss test or
a guarantee about storage hardware. Swift's directory-sync helper can log some
errors instead of propagating them; this experiment requires the successful
sync to be observed on the test filesystem.

## Experiment 5: composite unit-test regression

**Claim supported:** comparing upstream's composite builder tests before and
after the patch provides evidence that the change has not broken compose,
save, load, the persisted metadata format, relative-path saving, save errors,
or the cooperative builder.

```bash
python3 experiments/exp5_composite_unittests.py \
    --results-json exp5.json --timeout 600
```

Runs `test/unit/common/ring/test_composite_builder.py` with pytest, mirroring
experiment 0's environment, checkout, dependency, run, and result sections. It
accepts `--swift-src`, `SWIFT_SRC`, `--timeout` (600 seconds by default),
`--results-json`, `--dry-run`, and extra pytest arguments. A collection or import
error, such as a `TabError` in `composite_builder.py`, is reported as `77`
rather than as a result. A passing run reports **58 tests** (upstream's 43 plus
the 15 composite atomic-save unit tests described under
[Atomic-save unit tests](#atomic-save-unit-tests-in-swifts-test-suite)) and
`RESULT: PASS`.

**Limits:** none of upstream's composite tests interrupts or fails a save, so
this experiment cannot show that the bug is fixed; experiments 6–10 do that.
It is expected to pass on both the upstream and the patched tree.

## Experiment 6: composite partial serialization

**Claim tested:** if serialization writes only part of the composite metadata
and then raises an error, the previous composite file remains byte-identical,
loadable, and composes the same ring.

```bash
python3 experiments/exp6_composite_partial_dump.py
```

The single test saves a valid composite, then patches `json.dump` to write half
of its payload and raise `OSError`. It prints the file size and whether it loads
before and after, then checks the bytes, the loaded metadata, the composed
partition-to-device assignment, and that no temporary file remains. A passing
run reports **1 test**, `OK`, and `RESULT: PASS`.

**Expected comparison:** upstream truncates the live file, so it fails with
*"File does not contain valid composite ring data"*; the patched save passes.

**Limits:** the failure is injected, not an actual killed process. It does not
verify syncing, concurrent writers, or power-loss survival.

## Experiment 7: composite process interruption

**Claim tested:** until replacement, an interrupted composite save preserves the
previous file. After replacement, a process crash leaves the complete new file
available to the next reader.

```bash
python3 experiments/exp7_composite_interrupted_save.py
```

Four tests run saves of a changed composite in child processes. Each child
pauses at a controlled point and is killed by the parent with `SIGKILL`:

1. Halfway through serialization: the original bytes and loaded state survive.
2. After serialization, before the temporary file's `fsync`: the original
   survives.
3. Immediately before rename: the original survives.
4. Immediately after rename: the complete updated state loads successfully.

Each check also composes the ring again from the component builders and
compares its partition-to-device assignment. For kills before replacement, a
later save must work despite any temporary file left behind. A passing run
reports **4 tests**, `OK`, and `RESULT: PASS`.

**Expected comparison:** upstream fails all four: the partial file is visible
in the first case, and upstream never reaches an `fsync` or rename in the others.

**Limits:** a hard kill cannot run exception cleanup, so leftover temporary
files are permitted. The OS keeps running, so this does not establish
durability after power loss.

## Experiment 8: composite handled failures and cleanup

**Claim tested:** errors handled before replacement leave the previous
composite file unchanged and loadable, propagate to the caller, and remove the
temporary file created by that attempt.

```bash
python3 experiments/exp8_composite_save_failure_cleanup.py
```

Eight tests cover:

1. Serialization writing half the payload and raising an error.
2. The explicit file flush raising an error.
3. File `fsync` raising an error.
4. Replacement raising an error, including Swift's rename retry.
5. A first save failing: no composite file and no temporary file are created.
6. Cleanup itself failing: the original error, not the `unlink` error, is raised.
7. Saving an uncomposed builder: rejected with "No composed ring to save" without
   touching the file.
8. A retry after a failed replacement installs the update and leaves nothing behind.

Each injected failure must actually be reached and raise the expected error.
Existing-file checks compare the original bytes, loaded state, and composed
assignment, and require no extra files after cleanup. A passing run reports
**8 tests**, `OK`, and `RESULT: PASS`.

**Expected comparison:** upstream passes only test 7; the guard runs before the
file is opened. The others fail because the file is corrupted or because
upstream never reaches the injected flush, `fsync`, or rename.

**Limits:** this is caught-exception cleanup on a writable filesystem. It does
not require cleanup after `SIGKILL` or rollback after replacement has occurred.

## Experiment 9: composite successful-save durability ordering

**Claim tested:** changed composite metadata is prepared completely before
replacement, and a successful save follows the required file and directory sync
sequence.

```bash
python3 experiments/exp9_composite_save_durability_order.py
```

The single test observes real operations while saving a changed composite:

```text
serialize -> flush -> file_fsync -> close -> rename -> directory_fsync
```

It checks that the live file is never opened for writing; the temporary file
is a separate file in the destination directory; the original stays unchanged
until replacement; the temporary file holds the complete metadata when it is
synced and is closed before rename; and the synced directory descriptor
identifies the actual parent directory. The installed file must equal the
expected metadata, load, compose the same ring, and leave no file at the
temporary name. A passing run reports **1 test**, `OK`, and `RESULT: PASS`.

**Expected comparison:** upstream fails immediately with "save opened the live
composite file for writing".

**Limits:** as for experiment 4, observed sync calls are not a power-loss test,
and Swift's directory-sync helper can log some errors instead of raising them.

## Experiment 10: composite paths, repetition, and concurrent readers

**Claim tested:** composite saves work for bare relative and new paths, can be
repeated without leaving files behind, keep the persisted format, and never
expose a partial file to a concurrent reader.

```bash
python3 experiments/exp10_composite_save_edge_cases.py
```

Five tests cover:

1. A save given a bare file name in the current directory: the temporary file
   and the result are in that directory, and `saved_path` is absolute.
2. A save to a new path in another directory: the temporary file is created
   there, and the original composite is untouched.
3. 25 consecutive saves: each reloads at its version, and no temporary files
   remain.
4. The persisted JSON has exactly upstream's keys and values.
5. A separate reader process loads the composite continuously while 200 saves
   run; every load must succeed and see a version that was actually saved.

A passing run reports **5 tests**, `OK`, and `RESULT: PASS`.

**Expected comparison:** upstream passes tests 3 and 4. It fails tests 1 and 2,
which observe the temporary file, and almost certainly test 5. Test 5 depends
on timing, so an upstream pass there is not evidence of atomicity.

**Limits:** one writer only; no multiple concurrent writers, network
filesystems, or power loss.

## Experiment 11: composite file permissions

**Claim tested:** a save never makes the composite file more permissive. An
update may keep or remove permissions but must not add any, and a first save
must not exceed the mode of an ordinary file created in the same directory
under the current umask.

```bash
python3 experiments/exp11_composite_save_file_mode.py
```

Two tests cover an update starting from each of `0600`, `0640`, `0644`, `0660`,
and `0664` (as subtests), and a first save compared with a plainly created probe
file. Each prints the resulting mode. A passing run reports **2 tests**, `OK`,
and `RESULT: PASS`.

**Expected comparison:** both upstream and the patched tree pass. Upstream
keeps the existing mode; the patched save produces `0600`, which is narrower and
is accepted. The experiment records that side effect rather than proving it
harmless: other users lose read access to a saved composite file.

**Limits:** only the final file's permission bits are checked, not the
temporary file during the save, ownership, or ACLs.

## Test results

All experiments, 0–11, have been run on the team's `SwiftBugFix` Lightsail
instance (Ubuntu, Python 3.12.3) against the patched fork, and all pass.

| Experiment | Result |
| --- | --- |
| 0: unit suite | PASS |
| 1: partial serialization | PASS, 1 of 1 |
| 2: process interruption | PASS, 3 of 3 |
| 3: handled failures | PASS, 5 of 5 |
| 4: successful-save protocol | PASS, 1 of 1 |
| 5: composite unit suite | PASS, 58 of 58 |
| 6: partial serialization | PASS, 1 of 1 |
| 7: process interruption | PASS, 4 of 4 |
| 8: handled failures | PASS, 8 of 8 |
| 9: successful-save protocol | PASS, 1 of 1 |
| 10: paths, repetition, readers | PASS, 5 of 5; the reader made tens of thousands of loads across 200 saves without an error |
| 11: file permissions | PASS, 2 of 2 |
| Atomic-save unit tests (below) | PASS, 28 of 28; `flake8` clean |

## Atomic-save unit tests in Swift's test suite

The experiments' checks are also integrated into Swift's own unit suite, so they
run with every change in Swift's CI:

| File | Class | Tests |
| --- | --- | --- |
| `test/unit/common/ring/test_builder.py` | `TestRingBuilder` | 12 save tests: partial serialization; explicit-flush, `fsync`, rename, and failed-first-save failures; a failing cleanup; the durability order; a stale temporary file; a bare relative path; repeated saves; plus the existing `test_save` and `test_save_load`. |
| `test/unit/common/ring/test_composite_builder.py` | `TestCompositeRingBuilder` | 15 new tests: the same failures, a retry after failure, the durability order, readers seeing the old or new file across the rename, a stale temporary file, a new path, repeated saves, and two permission checks. |

```bash
python3 -m pytest test/unit/common/ring/test_builder.py \
    test/unit/common/ring/test_composite_builder.py -k save -v -p no:cacheprovider
flake8 swift/common/ring/builder.py swift/common/ring/composite_builder.py \
    test/unit/common/ring/test_builder.py test/unit/common/ring/test_composite_builder.py
```

Each test injects one failure with `mock.patch` around the real
`NamedTemporaryFile`, `os.fsync`, and `os.rename`, then checks the exact error,
byte-identical old contents, loading and placement, and that no temporary file
is left. The kill and concurrent-reader experiments are represented by
deterministic checks of the on-disk state at each step. The experiments remain
the evidence for real process kills, real concurrent readers, and before/after
comparisons against other checkouts.

## Source selection, output, and interpretation

Experiments 2–4 import the selected **real** Swift checkout and print its source
path and revision. They accept `--swift-src PATH`, `SWIFT_SRC`, `--timeout`
(default 90 seconds), and `--results-json PATH`. The default source is the fork
containing the scripts. `atomic_save_support.py` provides their shared runner
and fixtures; keep it beside all three scripts.

Experiments 6–11 behave the same way and accept the same options.
`composite_save_support.py` provides their shared runner and fixtures. It is
self-contained, not built on `atomic_save_support.py`, and creates a real
composite from two component builders in different regions. Keep it beside all
six scripts. Experiment 5, like experiment 0, is a standalone pytest runner.

```bash
python3 experiments/exp2_interrupted_save.py \
    --swift-src /path/to/prepared/checkout \
    --results-json exp2-results.json --timeout 90
```

For before/after evidence, run the same scripts against separate prepared
checkouts using `--swift-src`, record both revisions and source modifications,
and inspect the named failures. Do not switch a source checkout during a run.

| Exit code | Meaning |
| --- | --- |
| `0` | The selected checks ran and passed. |
| `1` | A check failed or errored; inspect the named test and traceback. |
| `77` | No valid test result was obtained, such as an unsupported host, missing source/dependencies, or timeout. |

The new experiments follow the existing runners' environment, checkout,
dependency, run, and result sections. Their elapsed times describe experiment
runtime, **not a benchmark of atomic-save overhead**. All experiments have been
run on Linux against the patched fork and pass; see [Test results](#test-results).

## Scope of the claims

Experiments 2–4 target only the patched individual `RingBuilder.save()`.
Experiments 5–11 target only the patched `CompositeRingBuilder.save()`; they do
not re-test the component builders' own saves. Together they do not establish
same-process concurrent-save isolation, multiple concurrent writers, actual
power-loss recovery, or running-cluster client behavior. Those broader claims
need separate implementation and evidence.
