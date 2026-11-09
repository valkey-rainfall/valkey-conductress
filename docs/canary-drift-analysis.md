# Canary Drift Analysis

*This document records the design of canary drift analysis and its operator
API as implemented; the command-line `--help` output and `config.py` are the
source of truth for current flags and defaults.*

## Overview

Ingests completed canary benchmark outcomes into immutable daily observations
and computes rolling median/MAD statistics to detect environmental drift over
time. Consumed by the canary status CLI and dashboard.

## Data Model

### canary_observations (schema v3)

Immutable daily observations keyed by `(runner_id, profile_id, profile_version,
utc_date, task_id)`. A partial unique index enforces at most one accepted
observation per `(runner_id, profile_id, profile_version, utc_date)`.

Each row stores:

- **score**: benchmark throughput (RPS). Must be positive and finite for
  acceptance; NULL for rejected observations.
- **phase**: `observation` (accepted 1–14), `calibrating` (15–27), `ready` (28+).
  Phase ordinal is computed from the total accepted count in the series,
  independent of the current UTC date. The summary phase uses `<=` for the
  observation boundary (i.e., the 14th sample is still `observation`; the 15th
  is the first `calibrating` sample).
- **accepted**: 1 for valid scores, 0 for malformed/non-finite/non-positive.
- **ref_median / ref_mad / delta_pct**: rolling statistics from *prior* window
  (strictly earlier UTC dates only).
- **series_ordinal**: the nth accepted observation in this series (1-based).
- **ref_sample_count**: count of strictly-earlier accepted observations used
  for reference statistics (never includes self).
- **candidate_warning_pct / candidate_alarm_pct**: per-platform thresholds
  resolved from the canary profile.
- **candidate_signal**: classification of this observation against candidate
  thresholds (`insufficient-data`, `within`, `warning`, `alarm`).
  Informational only in all phases.
- **actionable**: always 0 (never actionable during calibration or observation).
- **window_start / window_end**: date range of the reference window.
- **environment_json**: provenance fingerprint including runner_id, platform,
  provenance_schema_version, and optional host environment fields (kernel,
  instance_id, etc.).
- **env_change_annotation**: deterministic diff versus the latest earlier
  accepted observation's environment (NULL if unchanged or first observation).
- **provenance_schema_version**: schema version from the outcome record.

### canary_calibration_reports

One report per `(runner_id, profile_id, profile_version)`. Generated
automatically at the 28th accepted observation using exactly the first 28
accepted observations sorted deterministically by `(utc_date, task_id)`.

Report fields:

- **median_score / mad**: observed central tendency and dispersion.
- **robust_sigma**: `1.4826 * MAD` — consistent estimator of σ for normal data.
- **variability_floor_pct**: `3 * robust_sigma / |median| * 100` —
  the measurement-noise floor below which drift cannot be distinguished
  from random variation (≈99.7% of normal distribution).
- **candidate_warning_pct / candidate_alarm_pct**: per-platform thresholds
  from the canary profile (may be NULL if no profile match).
- **recommended_warning_pct**: `max(candidate_warning, variability_floor)`.
- **recommended_alarm_pct**: `max(candidate_alarm, 2 * variability_floor,
  recommended_warning)`.
- **status**: always `ready-for-review` — never automatically actionable.
- **zero_median_warning**: true when median is zero, requiring manual review.

## Algorithm

### Median

For even-length lists, the median is the **average of the two middle values**.
For odd-length lists, it is the single middle value.

### Rolling Window

1. Collect all accepted observations for the same `(runner, profile, version)`
   with `utc_date < current_date` (strict less-than: a sample cannot move its
   own baseline), ordered deterministically by `(utc_date, task_id)`.
2. Take the most recent 28 observations from that set.
3. Compute median and MAD (median absolute deviation) from the window.
4. Delta percentage: `(score - median) / |median| * 100`.

### Phase Progression

Phase ordinal uses the **total accepted count** in the series before insert
(independent of the current observation's UTC date), ensuring out-of-order
arrivals do not regress phase.

| Ordinal (nth accepted) | Phase | Behavior |
|---|---|---|
| 1–14 | `observation` | Accumulate data, no statistical assessment |
| 15–27 | `calibrating` | Statistics computed, calibration progress exposed |
| 28 | `ready` | Calibration report generated, system ready for review |
| 29+ | `ready` | Ongoing drift monitoring |

The report is generated when the 28th accepted observation arrives, regardless
of whether it is chronologically the latest.

### Candidate Signals

Each observation carries a `candidate_signal` derived from the profile's
per-platform thresholds:

- **insufficient-data**: no reference statistics or no candidate thresholds.
- **within**: `|delta_pct| < candidate_warning_pct`.
- **warning**: `candidate_warning_pct <= |delta_pct| < candidate_alarm_pct`.
- **alarm**: `|delta_pct| >= candidate_alarm_pct`.

Signals use absolute delta against candidate thresholds and are **informational
only** in every phase. `actionable` is always 0. No notifications are sent.

### Calibration Report Thresholds

At the 28-sample mark:

1. `robust_sigma = 1.4826 * MAD`
2. `variability_floor = 3 * robust_sigma / |median| * 100`
3. `recommended_warning = max(candidate_warning, variability_floor)`
4. `recommended_alarm = max(candidate_alarm, 2 * variability_floor, recommended_warning)`

### Edge Cases

- **MAD = 0**: all prior samples identical. Delta is computed normally.
- **Median = 0**: delta clamped to ±999999.0 when score ≠ 0; 0.0 when score = 0.
  Calibration variability floor is 0%; report includes `zero_median_warning: true`
  and status remains `ready-for-review` with an explicit warning message.
- **Non-finite score**: recorded as rejected (accepted=0), does not count toward
  any phase/window.
- **Non-positive score**: recorded as rejected (accepted=0). Canary benchmarks
  should always produce positive throughput.
- **Duplicate/replay**: idempotent by primary key; returns existing observation.
- **Same date, different task**: the partial unique index ensures at most one
  accepted observation per date. A different task_id for an already-accepted date
  returns the existing accepted observation without alteration.
- **Missed days**: gaps in the date sequence; no interpolation or backfill.
- **Out-of-order**: accepted and slotted by UTC date, not arrival order. Phase
  ordinal uses total series count, not calendar position.
- **Profile version change**: starts a completely independent series.
- **Malformed canary_id**: non-empty profile_id and utc_date are validated
  after the colon split; empty components are rejected with a warning.
- **Environment change**: a deterministic diff annotation is computed versus
  the latest earlier accepted observation's environment.

### Provenance

Each observation retains available provenance fields from the outcome record:

- `provenance_schema_version`: from the outcome's `provenance_schema_version`
  or `schema_version` field.
- `runner_id` and `platform`: resolved from the FleetRegistry.
- `environment`: any host-level environment fields (kernel, instance_id, etc.)
  from the result.

## Integration

The `ControlService.record_outcome()` method automatically calls
`DriftAnalyzer.ingest_outcome()` after a canary task completes successfully.
Ingestion failures are logged but do not block the task completion path.

Profile version is resolved **structurally** from the `CanaryProfileRegistry`
(matching profile_id). A regex fallback on the task note field is used only
for tests and legacy compatibility.

`CanaryProfileRegistry` and `FleetRegistry` are wired into `DriftAnalyzer`
from `create_app` via `ControlService`.

## Query Helpers

```python
analyzer = DriftAnalyzer(database)
analyzer.get_observation(runner_id, profile_id, version, date)  # accepted-only, deterministic
analyzer.get_observations(runner_id, profile_id, version, limit=60)
analyzer.get_all_observations(runner_id, profile_id, version)  # incl. rejected
analyzer.get_calibration_report(runner_id, profile_id, version)
analyzer.get_series_summary(runner_id, profile_id, version)
```

`get_observation` returns only accepted observations (deterministic, no
rejected results).

## DB Migration

Schema v3 adds `canary_observations` and `canary_calibration_reports` tables.
The `canary_observations` table includes a partial unique index enforcing at
most one accepted observation per `(runner, profile, version, date)`.
Migration is forward-compatible and idempotent (safe to run multiple times).

## Operator API

Authenticated read-only endpoints for canary status. Operator token required.
Not exposed on the public dashboard endpoint.

### GET /api/v1/canary/status

Fleet-wide canary status for all enabled runners.

**Response:**

```json
{
  "schema_version": 1,
  "generated_at": "2026-08-30T21:00:00Z",
  "runners": [
    {
      "runner_id": "armbench",
      "display_name": "Graviton 3",
      "platform": "arm64/c7g.metal/graviton3",
      "enabled": true,
      "canary_profile": "throughput-get-v1",
      "profile": {
        "profile_id": "throughput-get-v1",
        "profile_version": 1,
        "description": "Daily GET throughput canary",
        "source": "valkey",
        "pinned_commit": "fcd8bc3e...",
        "status": "active"
      },
      "schedule": [
        {"utc_date": "2026-08-30", "state": "created", "task_id": "...", "task_state": "completed"}
      ],
      "series": {
        "phase": "observation",
        "progress": "5/14",
        "accepted_count": 5,
        "rejected_count": 0
      },
      "latest_observation": {
        "utc_date": "2026-08-30",
        "score": 167000.0,
        "ref_median": 166500.0,
        "ref_mad": 300.0,
        "delta_pct": 0.5,
        "candidate_signal": "within",
        "actionable": 0
      },
      "calibration_report": null,
      "semantics": "observation-only"
    }
  ]
}
```

### GET /api/v1/canary/status/{runner_id}

Detailed canary status for one runner. Returns 404 for unknown runners.

**Response:**

```json
{
  "schema_version": 1,
  "generated_at": "...",
  "runner": { ... }
}
```

### Semantics field

| Value | Meaning |
|---|---|
| `no-profile` | Runner has no canary profile configured |
| `unknown-profile` | Profile ID in manifest not found in registry |
| `observation-only` | Series in observation/no-data phase |
| `calibrating` | Series collecting calibration samples |
| `ready-for-review` | Calibration report generated, awaiting human review |
| `non-actionable` | Ready phase, no review-pending report |

### No mutations

These endpoints perform no writes: no scheduler ticks, no task creation,
no observation ingestion. They are purely read-only aggregations.

## CLI

### conductress canary status

Show canary drift monitoring status for all enabled runners.

```bash
conductress canary status [--runner ID] [--json]
```

**Fleet summary (default):**

```
RUNNER         PROFILE                  PHASE            PROGRESS       SEMANTICS            LATEST
armbench       throughput-get-v1        observation      5/14           observation-only     2026-08-30 167,000 rps +0.50%
g4bench        throughput-get-v1        calibrating      20/28          calibrating          2026-08-30 209,000 rps -0.12%
```

**Detailed runner (--runner):**

Shows profile, series, schedule history, latest observation with
median/MAD/delta/thresholds/signal, environment provenance,
and calibration report when available.

**JSON output (--json):**

Preserves all API fields exactly. Wraps in the standard CLI envelope with
distinct command values per scope:

```json
{"schema_version": 1, "command": "canary.status.fleet", "data": { ... }}
{"schema_version": 1, "command": "canary.status.runner", "data": { ... }}
```

### Observation semantics

- Phases: `no-data` -> `observation` (1-14, inclusive) -> `calibrating` (15-27) -> `ready` (28+)
- `actionable` is always false -- no automated alerts
- Candidate signals are informational only
- Calibration reports require human review before enabling thresholds

## Findings

`control/findings.py` turns the canary history into ranked environment
EVENTS for the dashboard's canary page; `control/findings_sources.py` gathers
its inputs; the service publishes `canary/findings.json` and
`canary/consistency.json` (`docs/control-service.md`, "Canary findings").
Inputs per live runner: its accepted observations and those of its archives
(stitched, oldest first; the profile version is added to the environment),
the manual events and explanations of its platform's sweep manifest, and the
paired evidence of its closed series generations (the same commit and
workload measured before and after the cut), and its confirmation runs.

### Confirmation evidence

A confirmation run (`docs/configuration.md`, "Confirmation sweep") measures
old sweep points again on the new host. The data host reads
`confirm-<platform>.epoch-v3.json` from the sweep dir: each run with at least
one measured pair becomes one paired series named `confirmation run`, with
the run's event time as the cut, so it joins the event whose observation gap
holds that time and is tested like any paired series (5 pairs for a verdict
here; the run's own stop rule needs 3 pairs to confirm). The consistency
panel's `last_confirmation_run` is the newest run: `{date, pairs, status}`,
where `date` is the day it ended (else started), `pairs` the pairs measured
and `status` one of `requested, running, confirmed, cleared,
insufficient-data, no-pairs`; `null` when the runner has none. A run measures
only the new environment, so `cleared` means "the new host reproduces the old
numbers within the floor", not "the event changed nothing on any workload".

### Events

Every recorded field change between two consecutive accepted observations of
one runner, plus every manual event whose `at` falls in that gap, is ONE
event, identified as `<runner>:<date of the first observation after the
gap>:0`. No fixed-commit measurement lies between them, so no data can
separate them: the page attributes the effect to the event as a whole and
says so when several fields changed together. Identity fields (`runner_id`,
`platform`) are ignored, and a field that a newer provenance schema adds is
not a change. Kinds, first match wins: host replaced, kernel or firmware,
system library, load generator, canary profile, reboot only, harness
revision, manual, other change.

### Statistics

- Step location: robust binary segmentation restricted to event positions.
  Each round takes the event with the best L1 cost reduction
  (`sum |x - median|`) of its segment and accepts it as a step when the test
  below gives p < 0.01, `|effect|` reaches the runner floor, and the windows
  hold at least 7 days before and 3 after. Events whose split gains at least
  90% of an accepted step's gain are listed as alternative locations.
- Windows: before = up to 28 observations back to the previous accepted
  step, after = up to 28 observations up to the next accepted step. Later
  unaccepted events do not cut the window (harness deploys every few days
  would otherwise leave 1-day windows); the strict variant, cut at the next
  event of any kind, is reported beside it.
- Canary test: effect = median(after) / median(before) - 1, 95% bootstrap CI
  (10,000 resamples of both windows), two-sided Mann-Whitney by permutation
  of the rank sum (20,000 splits).
- Paired test: median of per-pair log ratios, bootstrap CI over pairs,
  Wilcoxon signed-rank by sign flips, share of pairs in the same direction.
- Benjamini-Hochberg over every test of one analysis.
- Floor per runner: `max(0.5%, 3 x robust sd)` of the canary before window
  (in-segment; when that window has fewer than 7 days, the in-segment after
  window, which a step at the event does not inflate; else the 28 prior days).
- Cross-runner: the effect on every other runner over the same dates, in
  robust sd.
- Fleet: harness deploys on at least 3 runners within 48 h get one pooled
  row, Stouffer `Z = sum(z_i) / sqrt(k)` of the strict effects.
- Seeds come from the test identity, so a re-analysis publishes the same
  numbers.

### States and ranking

| State | Rule |
|---|---|
| `confirmed` | at least 7 days before and 3 after (5 pairs for paired); q < 0.05; CI excludes 0; `|effect| >=` floor |
| `cleared` | CI inside +/- floor, with at least 3 days before, 3 after and 10 in total, so a flat event soon after a located step or at the start of the history is cleared too (`below_floor_shift: true` when q < 0.05 all the same) |
| `suspected` | otherwise, when p < 0.05 or `|effect| >=` 2 x floor |
| `insufficient-data` | minimum data not met, or no signal and the CI still too wide to clear; `reason` says which ("1 day(s) after the event, needs 3") |
| `explained` | an operator explanation exists for the event date (`conductress sweep explain-event`) |
| `suspected-fleet` | pooled fleet row with `|Z| >= 3`, at least 3 runners and 75% in one direction, and fewer than 3 days after the deploy on some runner |

An event takes the most serious state of its primary evidence (the canary,
and paired series with enough pairs); the canary leads the headline when it
has enough data. Severity = `|CI bound nearest 0| x (1 - q)` in percent, 0
when the CI holds 0. Order: confirmed, suspected, insufficient-data,
suspected-fleet, explained, cleared, then severity, then newest first.
`open_count` counts confirmed and suspected only. A pooled fleet row with too
little data is listed only for the latest deploy. A confirmed event carries a
`proposal`: the generation close a PERSON may perform; nothing does it
automatically. There is no proposal when the event is explained, or when a
closed generation already cuts the series at it (`generation_closed: true`:
the event owns paired evidence from that generation).

### Published values

The findings use the same allowlist as every public file
(`conductress/public_fields.py`): a field keeps its old and new values only
when `public_value_key` allows them (kernel, glibc, microcode, CPU model,
machine, package versions); every other field is published as the type of
change from `change_type_label` ("harness revision changed", "reboot", "host
replaced"). Operator text passes `public_text`.
