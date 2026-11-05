"""Human- and agent-friendly fleet control CLI."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any, Optional

from .fleet_client import FleetClient, FleetClientError

CLI_SCHEMA_VERSION = 1


def resolve_runner(client: FleetClient, *, runner_id: Optional[str], platform: Optional[str]) -> str:
    if bool(runner_id) == bool(platform):
        raise FleetClientError(
            "ROUTING_REQUIRED",
            "select exactly one of --runner or --platform",
            1,
        )
    runners = client.fleet().get("runners", [])
    if runner_id:
        matches = [runner for runner in runners if runner["runner_id"] == runner_id]
        if not matches:
            raise FleetClientError("RUNNER_NOT_FOUND", f"unknown runner: {runner_id}", 1)
        if not matches[0]["enabled"]:
            raise FleetClientError("RUNNER_DISABLED", f"runner is disabled: {runner_id}", 4)
        return runner_id

    matches = [
        runner
        for runner in runners
        if runner["enabled"] and (platform == runner["platform"] or platform in runner.get("platform_aliases", []))
    ]
    if not matches:
        raise FleetClientError("PLATFORM_NOT_FOUND", f"no enabled runner matches platform: {platform}", 1)
    if len(matches) > 1:
        raise FleetClientError(
            "PLATFORM_AMBIGUOUS",
            f"platform {platform} matches multiple runners; use --runner explicitly",
            4,
        )
    return matches[0]["runner_id"]


def _json_output(command: str, data: dict[str, Any]) -> None:
    print(
        json.dumps(
            {"schema_version": CLI_SCHEMA_VERSION, "command": command, "data": data},
            indent=2,
            sort_keys=True,
        )
    )


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _age(value: Optional[str]) -> str:
    parsed = _parse_time(value)
    if parsed is None:
        return "never"
    seconds = max(0, int((datetime.now(timezone.utc) - parsed).total_seconds()))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def _runner_state(runner: dict[str, Any]) -> str:
    updated = _parse_time(runner.get("status_updated_at"))
    ttl = runner.get("status_ttl_seconds", 900)
    if updated is None or (datetime.now(timezone.utc) - updated).total_seconds() > ttl:
        return "offline"
    counts = runner.get("task_counts", {})
    if counts.get("accepted", 0) or counts.get("claimed", 0):
        return "active"
    return "idle"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="conductress", description="Conductress fleet control")
    groups = parser.add_subparsers(dest="command", required=True)

    fleet = groups.add_parser("fleet", help="Discover and inspect benchmark runners")
    fleet_sub = fleet.add_subparsers(dest="fleet_command", required=True)
    for name, help_text in (
        ("list", "List fleet runners and platform aliases"),
        ("status", "Show runner status and task counts"),
    ):
        command = fleet_sub.add_parser(name, help=help_text)
        command.add_argument("--json", action="store_true")
    show = fleet_sub.add_parser("show", help="Show one runner in detail")
    show.add_argument("runner_id")
    show.add_argument("--json", action="store_true")

    remote = groups.add_parser("remote", help="Inspect and cancel remote tasks")
    remote_sub = remote.add_subparsers(dest="remote_command", required=True)

    canary = groups.add_parser("canary", help="Canary drift monitoring")
    canary_sub = canary.add_subparsers(dest="canary_command", required=True)
    canary_status = canary_sub.add_parser("status", help="Show canary status for all runners or one runner")
    canary_status.add_argument("--runner", dest="runner_id", help="Show detailed status for one runner")
    canary_status.add_argument("--json", action="store_true")
    remote_list = remote_sub.add_parser("list", help="List remote tasks")
    remote_list.add_argument("--runner")
    remote_list.add_argument("--state")
    remote_list.add_argument("--limit", type=int, default=50)
    remote_list.add_argument("--offset", type=int, default=0)
    remote_list.add_argument("--json", action="store_true")
    remote_show = remote_sub.add_parser("show", help="Show a remote task")
    remote_show.add_argument("task_id")
    remote_show.add_argument("--json", action="store_true")
    remote_cancel = remote_sub.add_parser("cancel", help="Cancel a queued remote task")
    remote_cancel.add_argument("task_id")
    remote_cancel.add_argument("--json", action="store_true")
    return parser


def _fleet_list(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.fleet()
    if args.json:
        _json_output("fleet.list", document)
        return 0
    print(f"{'RUNNER':<14} {'PLATFORM':<42} {'ALIASES':<24} ENABLED")
    for runner in document.get("runners", []):
        aliases = ", ".join(runner.get("platform_aliases", [])) or "-"
        enabled = "yes" if runner["enabled"] else "no"
        print(f"{runner['runner_id']:<14} {runner['platform']:<42} {aliases:<24} {enabled}")
    return 0


def _fleet_status(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.fleet()
    if args.json:
        _json_output("fleet.status", document)
        return 0
    print(f"{'RUNNER':<14} {'STATE':<9} {'QUEUED':>7} {'ACTIVE':>7} {'AGE':>8}  PLATFORM")
    for runner in document.get("runners", []):
        counts = runner.get("task_counts", {})
        active = counts.get("claimed", 0) + counts.get("accepted", 0)
        print(
            f"{runner['runner_id']:<14} {_runner_state(runner):<9} "
            f"{counts.get('queued', 0):>7} {active:>7} "
            f"{_age(runner.get('status_updated_at')):>8}  {runner['platform']}"
        )
    return 0


def _fleet_show(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.runner(args.runner_id)
    if args.json:
        _json_output("fleet.show", document)
        return 0
    runner = document["runner"]
    print(f"Runner:      {runner['runner_id']} ({runner['display_name']})")
    print(f"Platform:    {runner['platform']}")
    print(f"Aliases:     {', '.join(runner.get('platform_aliases', [])) or '-'}")
    print(f"Enabled:     {'yes' if runner['enabled'] else 'no'}")
    print(f"State:       {_runner_state(runner)}")
    print(f"Status age:  {_age(runner.get('status_updated_at'))}")
    counts = runner.get("task_counts", {})
    print(f"Task counts: {', '.join(f'{key}={value}' for key, value in sorted(counts.items())) or '-'}")
    if runner.get("status"):
        current = runner["status"].get("current_task")
        print(f"Current task: {current.get('id') if current else '-'}")
        fleet_control = runner["status"].get("fleet_control") or {}
        if fleet_control.get("last_error"):
            print(f"Last error:  {fleet_control['last_error']}")
        downgrade = fleet_control.get("last_outcome_downgrade")
        if downgrade:
            # An outcome the runner could not deliver in full: trimmed before
            # the send, reduced or failed after a rejection, or dropped.
            print(
                f"Last outcome: {downgrade.get('stage')} for {downgrade.get('task_id')} "
                f"at {downgrade.get('at')}: {downgrade.get('reason')}"
            )
    return 0


def _remote_list(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.list_tasks(
        runner_id=args.runner,
        state=args.state,
        limit=args.limit,
        offset=args.offset,
    )
    if args.json:
        _json_output("remote.list", document)
        return 0
    tasks = document.get("tasks", [])
    if not tasks:
        print("No remote tasks found.")
        return 0
    print(f"{'TASK ID':<31} {'RUNNER':<14} {'CLASS':<9} {'STATE':<10} PRIORITY  SUBMITTED")
    for task in tasks:
        print(
            f"{task['task_id']:<31} {task['runner_id']:<14} {task['task_class']:<9} "
            f"{task['state']:<10} {task['priority']:>8}  {task['submitted_at']}"
        )
    return 0


def _remote_show(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.task(args.task_id)
    if args.json:
        _json_output("remote.show", document)
        return 0
    task = document["task"]
    print(f"Task:        {task['task_id']}")
    print(f"Runner:      {task['runner_id']}")
    print(f"Class/state: {task['task_class']} / {task['state']}")
    print(f"Priority:    {task['priority']}")
    print(f"Submitted:   {task['submitted_at']} by {task['submitted_by']}")
    print(f"Task type:   {task['envelope']['task']['task_type']}")
    if task.get("outcome"):
        outcome = task["outcome"]
        print(f"Outcome:     {outcome['state']} at {outcome['completed_at']}")
        result = outcome.get("result") or {}
        score = result.get("score")
        if isinstance(score, (int, float)):
            print(f"Score:       {int(score):,} rps")
        elif score is not None:
            print(f"Score:       {score}")
        cv = result.get("cv")
        if isinstance(cv, (int, float)):
            print(f"CV:          {cv:.4f}")
        reps = result.get("reps")
        if reps is not None:
            print(f"Reps:        {reps}")
        smin = result.get("score_min")
        smax = result.get("score_max")
        if isinstance(smin, (int, float)) and isinstance(smax, (int, float)):
            print(f"Score range: {int(smin):,} - {int(smax):,} rps")
    return 0


def _remote_cancel(client: FleetClient, args: argparse.Namespace) -> int:
    document = client.cancel_task(args.task_id)
    if args.json:
        _json_output("remote.cancel", document)
        return 0
    tasks = document.get("tasks", [])
    changed = document.get("changed", 0)
    print(f"Cancel {args.task_id}: {changed} task(s) changed")
    for task in tasks:
        print(f"  {task['task_id']}: {task['state']}")
    return 0


def _canary_status(client: FleetClient, args: argparse.Namespace) -> int:
    if args.runner_id:
        document = client.canary_status_runner(args.runner_id)
        if args.json:
            _json_output("canary.status.runner", document)
            return 0
        _print_canary_runner_detail(document.get("runner", {}))
        return 0

    document = client.canary_status()
    if args.json:
        _json_output("canary.status.fleet", document)
        return 0
    _print_canary_fleet_summary(document.get("runners", []))
    return 0


def _print_canary_fleet_summary(runners: list[dict[str, Any]]) -> None:
    if not runners:
        print("No enabled runners with canary profiles.")
        return
    # Compact ~80-col table: RUNNER PROFILE STATE PROGRESS LATEST
    hdr = f"{'RUNNER':<12} {'PROFILE':<18} {'STATE':<14} {'PROG':>7}  LATEST"
    print(hdr)
    for r in runners:
        rid = r.get("runner_id", "?")[:12]
        profile_id = (r.get("canary_profile") or "-")[:18]
        series = r.get("series") or {}
        phase = series.get("phase", "-")[:14]
        progress = (series.get("progress") or "-")[:7]
        latest = r.get("latest_observation")
        if latest:
            score = latest.get("score")
            delta = latest.get("delta_pct")
            signal = latest.get("candidate_signal", "")
            score_str = f"{int(score):,}" if isinstance(score, (int, float)) else "?"
            delta_str = f"{delta:+.1f}%" if isinstance(delta, (int, float)) else ""
            sig_str = f" [{signal}]" if signal and signal not in ("within", "insufficient-data") else ""
            latest_str = f"{score_str} rps {delta_str}{sig_str}"
        else:
            latest_str = "-"
        print(f"{rid:<12} {profile_id:<18} {phase:<14} {progress:>7}  {latest_str}")


def _print_canary_runner_detail(runner: dict[str, Any]) -> None:
    rid = runner.get("runner_id", "?")
    print(f"Runner:      {rid} ({runner.get('display_name', '?')})")
    print(f"Platform:    {runner.get('platform', '?')}")
    print(f"Enabled:     {'yes' if runner.get('enabled') else 'no'}")

    profile = runner.get("profile")
    if not profile:
        print("Canary:      no profile configured")
        return
    if profile.get("status") == "unknown":
        print(f"Canary:      profile {profile.get('profile_id', '?')} (unknown/not loaded)")
        return

    pinned = profile.get("pinned_commit") or "none"
    pinned_display = pinned[:12] if len(pinned) >= 12 else pinned
    print(f"Profile:     {profile['profile_id']} v{profile.get('profile_version', '?')}")
    print(f"Description: {profile.get('description') or '-'}")
    print(f"Source:      {profile.get('source', '?')} @ {pinned_display}")
    print(f"Semantics:   {runner.get('semantics', '?')}")

    # Series summary
    series = runner.get("series") or {}
    phase = series.get("phase", "no-data")
    progress = series.get("progress") or "-"
    accepted = series.get("accepted_count", 0)
    rejected = series.get("rejected_count", 0)
    print(f"\nDrift series:")
    print(f"  Phase:     {phase} ({progress})")
    print(f"  Accepted:  {accepted}")
    print(f"  Rejected:  {rejected}")

    # Schedule
    schedule = runner.get("schedule") or []
    if schedule:
        print(f"\nRecent schedule ({len(schedule)} entries):")
        for s in schedule[:7]:
            task_state = s.get("task_state") or "-"
            tid = s.get("task_id") or "-"
            # Truncate task_id for compact display
            tid_display = tid[:36] if len(tid) > 36 else tid
            print(f"  {s['utc_date']}  {s['state']:<10} task={tid_display:<36}  {task_state}")

    # Latest observation
    latest = runner.get("latest_observation")
    if latest:
        print(f"\nLatest accepted observation:")
        print(f"  Date:      {latest.get('utc_date', '?')}")
        score = latest.get("score")
        print(f"  Score:     {int(score):,} rps" if isinstance(score, (int, float)) else f"  Score:     {score}")
        ref_median = latest.get("ref_median")
        ref_mad = latest.get("ref_mad")
        if ref_median is not None:
            print(f"  Median:    {int(ref_median):,} (prior rolling)")
        if ref_mad is not None:
            print(f"  MAD:       {ref_mad:,.2f}")
        delta = latest.get("delta_pct")
        if delta is not None:
            print(f"  Delta:     {delta:+.4f}%")
        cw = latest.get("candidate_warning_pct")
        ca = latest.get("candidate_alarm_pct")
        if cw is not None or ca is not None:
            print(f"  Thresholds: warning={cw}% alarm={ca}%")
        signal = latest.get("candidate_signal")
        if signal:
            print(f"  Signal:    {signal}")

        # Print actual actionable value from the API
        actionable_val = latest.get("actionable")
        print(f"  Actionable: {actionable_val}")
        if not actionable_val:
            print("  (non-actionable by design in current phase)")

        env = latest.get("environment")
        env_change = latest.get("env_change_annotation")
        if env_change:
            print(f"  Env change: {env_change}")
        provenance = latest.get("provenance_schema_version")
        if provenance is not None:
            print(f"  Provenance: schema v{provenance}")
    else:
        print(f"\nNo accepted observations yet.")

    # Calibration report
    cal = runner.get("calibration_report")
    if cal:
        print(f"\nCalibration report ({cal.get('status', '?')}):")
        print(f"  Samples:    {cal.get('sample_count', '?')}")
        dr = cal.get("date_range", {})
        print(f"  Date range: {dr.get('start', '?')} - {dr.get('end', '?')}")
        ms = cal.get("median_score")
        print(f"  Median:     {int(ms):,}" if isinstance(ms, (int, float)) else f"  Median:     {ms}")
        print(f"  MAD:        {cal.get('mad')}")
        print(f"  Robust sigma: {cal.get('robust_sigma')}")
        vf = cal.get("variability_floor_pct")
        print(f"  Var floor:  {vf:.4f}%" if isinstance(vf, (int, float)) else f"  Var floor:  {vf}")
        rw = cal.get("recommended_warning_pct")
        ra = cal.get("recommended_alarm_pct")
        print(f"  Recommended: warning={rw}% alarm={ra}%")
        print("  Review and accept before enabling drift alerts.")
    elif accepted > 0:
        cal_needed = series.get("calibration_samples_required", 28)
        remaining = max(0, cal_needed - accepted)
        print(f"\nCalibration: {remaining} more observations needed for report.")


def main(argv: Optional[list[str]] = None, *, client: Optional[FleetClient] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    json_mode = bool(getattr(args, "json", False))
    try:
        client = client or FleetClient.from_env()
        if args.command == "fleet":
            if args.fleet_command == "list":
                return _fleet_list(client, args)
            if args.fleet_command == "status":
                return _fleet_status(client, args)
            return _fleet_show(client, args)
        if args.command == "canary":
            return _canary_status(client, args)
        if args.remote_command == "list":
            return _remote_list(client, args)
        if args.remote_command == "show":
            return _remote_show(client, args)
        return _remote_cancel(client, args)
    except FleetClientError as exc:
        if json_mode:
            payload = {
                "schema_version": CLI_SCHEMA_VERSION,
                "error": True,
                "code": exc.code,
                "message": exc.message,
                "exit_code": exc.exit_code,
            }
            if exc.details is not None:
                payload["details"] = exc.details
            print(json.dumps(payload, sort_keys=True))
        else:
            print(f"Error [{exc.code}]: {exc.message}", file=sys.stderr)
        return exc.exit_code
