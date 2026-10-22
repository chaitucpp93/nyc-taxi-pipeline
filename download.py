"""Download one file safely: validated, retried, atomically published, idempotent."""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import requests

log = logging.getLogger("download")

TRANSIENT_STATUS = {429, 500, 502, 503, 504}
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


class TruncatedDownload(Exception):
    """Fewer bytes arrived than the server promised."""


def is_transient(exc: BaseException) -> bool:
    """Retry only failures that can fix themselves (step 4)."""
    if isinstance(
        exc,
        (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,  # connection dropped mid-file
            TruncatedDownload,
        ),
    ):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code in TRANSIENT_STATUS
    return False  # 403/404 etc. give the same answer every time


def fetch_once(session: requests.Session, url: str, dest: Path) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with session.get(
            url,
            stream=True,
            timeout=(10, 60),                       # (connect, read) timeouts
            headers={"Accept-Encoding": "identity"},  # promised size == bytes written
        ) as response:
            response.raise_for_status()               # step 1
            promised = response.headers.get("Content-Length")
            with open(tmp, "wb") as fh:               # step 2: build in the "back room"
                for chunk in response.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)


        received = tmp.stat().st_size
        if received == 0:
            raise TruncatedDownload(f"empty download from {url}")
        if promised is not None and received != int(promised):   # step 3
            raise TruncatedDownload(f"promised {promised}, received {received}")

        os.replace(tmp, dest)                         # step 2: publish only after validation
    except BaseException:
        tmp.unlink(missing_ok=True)                   # never leave debris, never swallow the error
        raise


def download(
    url: str,
    dest: Path,
    *,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    session: requests.Session | None = None,
) -> bool:
    """Return True if downloaded, False if skipped."""
    if dest.exists():                                 # step 5: safe because of steps 2 and 3
        log.info("skip (already present): %s", dest)
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    session = session or requests.Session()

    for attempt in range(1, max_attempts + 1):        # step 4
        try:
            fetch_once(session, url, dest)
            log.info("downloaded %s (%d bytes)", dest, dest.stat().st_size)
            return True
        except Exception as exc:                      # Ctrl-C is not an Exception: passes through
            if not is_transient(exc) or attempt == max_attempts:
                raise
            delay = base_delay * 2 ** (attempt - 1)   # 1s, 2s, 4s, 8s...
            log.warning("attempt %d/%d failed: %s; retrying in %.0fs", attempt, max_attempts, exc, delay)
            time.sleep(delay)
    raise AssertionError("unreachable")               # satisfies the type checker


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    download(ZONE_URL, Path("data/taxi_zone_lookup.csv"))