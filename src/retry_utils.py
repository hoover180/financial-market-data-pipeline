import time
import random
import logging
import requests

logger = logging.getLogger("extract")

# 429 and 5xx are transient — retrying helps. Other 4xx (bad request, bad
# API key, not found) won't resolve on retry, so fail fast instead of
# wasting the retry budget on an outcome that can't change.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_retryable_http_error(exc):
    response = getattr(exc, "response", None)
    if response is None:
        return True  # connection/timeout errors carry no response object
    return response.status_code in RETRYABLE_STATUS_CODES


def with_retry(max_attempts=4, base_delay_seconds=2, max_delay_seconds=30):
    def decorator(func):
        def wrapper(*args, **kwargs):
            delay = base_delay_seconds
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as exc:
                    if not _is_retryable_http_error(exc) or attempt == max_attempts:
                        raise
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout) as exc:
                    if attempt == max_attempts:
                        raise
                sleep_for = min(delay, max_delay_seconds) + random.uniform(0, 1)
                logger.warning(
                    "%s attempt %d/%d failed, retrying in %.1fs",
                    func.__name__, attempt, max_attempts, sleep_for,
                )
                time.sleep(sleep_for)
                delay *= 2
        return wrapper
    return decorator