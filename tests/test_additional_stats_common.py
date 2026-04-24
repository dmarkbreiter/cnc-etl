import requests

from flows.additional_stats_common import fetch_additional_stat


class _LoggerStub:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, tuple[object, ...]]] = []

    def warning(self, message: str, *args: object) -> None:
        self.warnings.append((message, args))

    def info(self, message: str, *args: object) -> None:
        return None


class _ResponseStub:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def test_fetch_additional_stat_returns_defaults_for_deleted_project(monkeypatch):
    logger = _LoggerStub()
    sleeps: list[float] = []

    def fake_fetch_stat_value(_project_id: int, _stat_name: str):
        raise requests.HTTPError("422 error", response=_ResponseStub(422))

    monkeypatch.setattr(
        "flows.additional_stats_common._fetch_stat_value",
        fake_fetch_stat_value,
    )
    monkeypatch.setattr("flows.additional_stats_common.get_run_logger", lambda: logger)
    monkeypatch.setattr("flows.additional_stats_common.time.sleep", sleeps.append)

    actual = fetch_additional_stat.fn(
        270644,
        stat_name="quality_grades",
        api_call_delay=0.25,
    )

    assert actual == {
        "id": 270644,
        "quality_grades": {"research": 0, "needs_id": 0, "casual": 0},
    }
    assert sleeps == [0.25]
    assert logger.warnings == [
        (
            "Project %s returned 422 for %s; using default value because the project may have been deleted.",
            (270644, "quality_grades"),
        )
    ]


def test_fetch_additional_stat_reraises_non_422_http_errors(monkeypatch):
    def fake_fetch_stat_value(_project_id: int, _stat_name: str):
        raise requests.HTTPError("500 error", response=_ResponseStub(500))

    monkeypatch.setattr(
        "flows.additional_stats_common._fetch_stat_value",
        fake_fetch_stat_value,
    )
    monkeypatch.setattr(
        "flows.additional_stats_common.get_run_logger",
        lambda: _LoggerStub(),
    )

    try:
        fetch_additional_stat.fn(270644, stat_name="quality_grades", api_call_delay=0.0)
    except requests.HTTPError as exc:
        assert exc.response.status_code == 500
    else:
        raise AssertionError("Expected HTTPError to be re-raised for non-422 responses")
