from datetime import datetime, timezone

from apscheduler.triggers.cron import CronTrigger

from .config import APP_TIMEZONE


def next_run_from_cron(cron_expr, base_time=None):
    base_time = base_time or datetime.now(timezone.utc)
    local_base = base_time.astimezone(APP_TIMEZONE)
    trigger = CronTrigger.from_crontab(cron_expr, timezone=APP_TIMEZONE)
    next_run = trigger.get_next_fire_time(None, local_base)
    if not next_run:
        raise ValueError("cron 表达式无法计算下次运行时间")
    return next_run.astimezone(timezone.utc).replace(microsecond=0).isoformat()
