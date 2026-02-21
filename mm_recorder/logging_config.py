import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

def setup_logging(
    level: str = "INFO",
    component: str = "app",
    subdir: str = "default",
    base_dir: str | Path = "logs",
) -> Path:
    """
    Configure logging:
      - Console (stdout)
      - Daily log file in logs/<component>/<symbol>/YYYY-MM-DD.log (Berlin date)
      - Also keeps rotated backups if the process spans midnight

    Returns:
      Path to the "current" daily log file.
    """

    log_dir = Path(base_dir) / component / subdir
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d")
    log_path = log_dir / f"{date_str}.log"

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)
    return log_path


def setup_run_logging(
    *,
    level: str = "INFO",
    run_type: str,
    symbol: str,
    yyyymmdd: str,
    run_id: str,
    base_dir: str | Path = "out/logs",
    method: Optional[str] = None,
) -> Path:
    """Configure run-scoped logging for batch jobs.

    The intent is to make logs reproducible and collocated with run artifacts.

    Default layout:
      out/logs/<run_type>/<method?>/<symbol>/<yyyymmdd>/<run_id>/run.log
    """
    parts = [run_type]
    if method:
        parts.append(method)
    parts.extend([symbol, yyyymmdd, run_id])
    log_dir = Path(base_dir).joinpath(*parts)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "run.log"

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)
    return log_path
