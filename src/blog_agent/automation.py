from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo


@dataclass(slots=True)
class AutomationSettings:
    enabled: bool = True
    daily_time: str = "09:00"
    timezone: str = "Asia/Kolkata"
    run_now: bool = False
    last_run_at: str = ""
    next_run_at: str = ""


@dataclass(slots=True)
class AutomationDecision:
    should_run: bool
    reason: str
    next_run_at: str


def evaluate_automation_schedule(
    settings: AutomationSettings,
    *,
    now_utc: datetime | None = None,
) -> AutomationDecision:
    now_utc = (now_utc or datetime.now(UTC)).astimezone(UTC)
    tz = _safe_zoneinfo(settings.timezone)
    local_now = now_utc.astimezone(tz)

    daily_hour, daily_minute = _parse_daily_time(settings.daily_time)
    today_target = local_now.replace(
        hour=daily_hour,
        minute=daily_minute,
        second=0,
        microsecond=0,
    )

    if settings.run_now:
        next_run = _compute_next_run_iso(
            local_now=local_now,
            daily_hour=daily_hour,
            daily_minute=daily_minute,
            force_tomorrow=True,
        )
        return AutomationDecision(
            should_run=True,
            reason="run_now",
            next_run_at=next_run,
        )

    if not settings.enabled:
        next_run = _compute_next_run_iso(
            local_now=local_now,
            daily_hour=daily_hour,
            daily_minute=daily_minute,
        )
        return AutomationDecision(
            should_run=False,
            reason="disabled",
            next_run_at=next_run,
        )

    last_run_local = _parse_iso(settings.last_run_at, tz)
    if local_now < today_target:
        next_run = today_target.astimezone(UTC).isoformat()
        return AutomationDecision(
            should_run=False,
            reason="before_daily_window",
            next_run_at=next_run,
        )

    if last_run_local and last_run_local.date() == local_now.date():
        next_run = _compute_next_run_iso(
            local_now=local_now,
            daily_hour=daily_hour,
            daily_minute=daily_minute,
            force_tomorrow=True,
        )
        return AutomationDecision(
            should_run=False,
            reason="already_ran_today",
            next_run_at=next_run,
        )

    next_run = _compute_next_run_iso(
        local_now=local_now,
        daily_hour=daily_hour,
        daily_minute=daily_minute,
        force_tomorrow=True,
    )
    return AutomationDecision(
        should_run=True,
        reason="daily_window_due",
        next_run_at=next_run,
    )


def build_post_run_updates(
    settings: AutomationSettings,
    *,
    now_utc: datetime | None = None,
) -> dict[str, str | bool]:
    now_utc = (now_utc or datetime.now(UTC)).astimezone(UTC)
    tz = _safe_zoneinfo(settings.timezone)
    local_now = now_utc.astimezone(tz)
    daily_hour, daily_minute = _parse_daily_time(settings.daily_time)
    next_run = _compute_next_run_iso(
        local_now=local_now,
        daily_hour=daily_hour,
        daily_minute=daily_minute,
        force_tomorrow=True,
    )
    return {
        "runNow": False,
        "lastRunAt": now_utc.isoformat(),
        "nextRunAt": next_run,
    }


def _parse_daily_time(value: str) -> tuple[int, int]:
    cleaned = str(value or "").strip()
    if not cleaned:
        return 9, 0
    parts = cleaned.split(":", 1)
    if len(parts) != 2:
        return 9, 0
    try:
        hour = max(0, min(23, int(parts[0])))
        minute = max(0, min(59, int(parts[1])))
        return hour, minute
    except ValueError:
        return 9, 0


def _parse_iso(value: str, tz: ZoneInfo) -> datetime | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(tz)


def _safe_zoneinfo(value: str) -> ZoneInfo:
    try:
        return ZoneInfo(str(value or "").strip() or "UTC")
    except Exception:  # noqa: BLE001
        return ZoneInfo("UTC")


def _compute_next_run_iso(
    *,
    local_now: datetime,
    daily_hour: int,
    daily_minute: int,
    force_tomorrow: bool = False,
) -> str:
    candidate = local_now.replace(
        hour=daily_hour,
        minute=daily_minute,
        second=0,
        microsecond=0,
    )
    if force_tomorrow or candidate <= local_now:
        candidate = candidate + timedelta(days=1)
    return candidate.astimezone(UTC).isoformat()
