from unittest.mock import MagicMock
import sys
from pathlib import Path
import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from extract import with_retry, _is_retryable_http_error


def make_http_error(status_code):
    resp = MagicMock()
    resp.status_code = status_code
    return requests.exceptions.HTTPError(response=resp)


def test_retries_on_429_then_succeeds():
    calls = {"n": 0}

    @with_retry(max_attempts=4, base_delay_seconds=0.01, max_delay_seconds=0.05)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise make_http_error(429)
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


def test_fails_fast_on_non_retryable_404():
    calls = {"n": 0}

    @with_retry(max_attempts=4, base_delay_seconds=0.01, max_delay_seconds=0.05)
    def bad_request():
        calls["n"] += 1
        raise make_http_error(404)

    with pytest.raises(requests.exceptions.HTTPError):
        bad_request()
    assert calls["n"] == 1


def test_exhausts_attempts_on_persistent_failure():
    calls = {"n": 0}

    @with_retry(max_attempts=3, base_delay_seconds=0.01, max_delay_seconds=0.05)
    def always_down():
        calls["n"] += 1
        raise make_http_error(503)

    with pytest.raises(requests.exceptions.HTTPError):
        always_down()
    assert calls["n"] == 3