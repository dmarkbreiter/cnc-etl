import requests

from results.fetch import (
    get_non_inaturalist_project_stats,
    get_project_identifiers_count,
    get_project_most_observed_species,
    get_project_quality_grades,
    get_strapi_results,
    get_umbrella_project_stats,
)


class _StubResponse:
    def __init__(self, payload: dict):
        self._payload = payload
        self.status_code = 200
        self.headers: dict[str, str] = {}
        self.text = ""

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _HttpResponseStub:
    def __init__(
        self,
        *,
        status_code: int,
        payload: dict,
        headers: dict[str, str] | None = None,
        text: str = "",
    ):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code > 399:
            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def json(self) -> dict:
        return self._payload


def test_get_strapi_results_normalizes_strapi_payload(monkeypatch):
    calls = {}

    def fake_get(
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        timeout: float | None = None,
    ):
        calls["url"] = url
        calls["params"] = params
        calls["headers"] = headers
        calls["timeout"] = timeout
        return _StubResponse(
            {
                "data": [
                    {
                        "attributes": {
                            "results": [
                                {
                                    "display": "Los Angeles",
                                    "project_id": 123,
                                    "observation_count": 456,
                                    "species_count": 78,
                                    "observers_count": 90,
                                    "identifiers_count": "12",
                                },
                                {
                                    "display": "Long Beach",
                                    "project_id": 456,
                                    "observation_count": 10,
                                    "species_count": 5,
                                    "observers_count": 3,
                                    "identifiers_count": None,
                                },
                            ]
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr("results.fetch.requests.get", fake_get)

    actual = get_strapi_results(2026)

    assert calls == {
        "url": "https://cnc.nhmlac.org/api/event-dates",
        "params": {
            "populate[0]": "results",
            "populate[results][populate][most_observed_species][populate]": "*",
            "filters[year][$eq]": 2026,
        },
        "headers": {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/html,*/*"},
        "timeout": 30,
    }
    assert actual == [
        {
            "display": "Los Angeles",
            "project_id": 123,
            "observation_count": 456,
            "species_count": 78,
            "observers_count": 90,
            "identifiers_count": "12",
        },
        {
            "display": "Long Beach",
            "project_id": 456,
            "observation_count": 10,
            "species_count": 5,
            "observers_count": 3,
            "identifiers_count": None,
        },
    ]


def test_get_strapi_results_unwraps_nested_strapi_objects(monkeypatch):
    def fake_get(
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        timeout: float | None = None,
    ):
        return _StubResponse(
            {
                "data": [
                    {
                        "id": 10,
                        "attributes": {
                            "results": [
                                {
                                    "id": 99,
                                    "attributes": {
                                        "display": "Nested City",
                                        "observation_count": 11,
                                        "species_count": 12,
                                        "observers_count": 13,
                                        "identifiers_count": "14",
                                        "project": {
                                            "data": {
                                                "id": 777,
                                                "attributes": {
                                                    "title": "Nested Project",
                                                    "slug": "nested-project",
                                                },
                                            }
                                        },
                                        "region": {
                                            "data": {
                                                "id": 42,
                                                "attributes": {
                                                    "name": "Southern California",
                                                },
                                            }
                                        },
                                    },
                                }
                            ]
                        },
                    }
                ]
            }
        )

    monkeypatch.setattr("results.fetch.requests.get", fake_get)

    actual = get_strapi_results(2026)

    assert actual == [
        {
            "id": 99,
            "display": "Nested City",
            "observation_count": 11,
            "species_count": 12,
            "observers_count": 13,
            "identifiers_count": "14",
            "project": {
                "title": "Nested Project",
                "slug": "nested-project",
                "id": 777,
            },
            "region": {
                "name": "Southern California",
                "id": 42,
            },
        }
    ]


def test_get_project_quality_grades_retries_on_rate_limit(monkeypatch):
    calls: list[dict] = []
    sleeps: list[float] = []
    responses = iter(
        [
            _HttpResponseStub(
                status_code=429,
                payload={},
                headers={"Retry-After": "0"},
                text="normal_throttling",
            ),
            _HttpResponseStub(
                status_code=200,
                payload={
                    "results": [
                        {"quality_grade": "research", "count": 12},
                        {"quality_grade": "casual", "count": 3},
                    ]
                },
            ),
        ]
    )

    def fake_get(
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        timeout: float | None = None,
    ):
        calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return next(responses)

    monkeypatch.setattr("results.fetch.requests.get", fake_get)
    monkeypatch.setattr("results.fetch.time_module.sleep", sleeps.append)

    actual = get_project_quality_grades(265959)

    assert len(calls) == 2
    assert sleeps == [5.0]
    assert actual == {"research": 12, "needs_id": 0, "casual": 3}


def test_get_project_most_observed_species_retries_on_rate_limit(monkeypatch):
    responses = iter(
        [
            _HttpResponseStub(
                status_code=429,
                payload={},
                headers={"Retry-After": "0"},
                text="normal_throttling",
            ),
            _HttpResponseStub(
                status_code=200,
                payload={
                    "results": [
                        {
                            "count": 9,
                            "taxon": {
                                "name": "Danaus plexippus",
                                "preferred_common_name": "monarch",
                                "default_photo": {
                                    "medium_url": "https://example.com/photo.jpg",
                                    "attribution": "Example",
                                    "original_dimensions": {"width": 100, "height": 80},
                                },
                            },
                        }
                    ]
                },
            ),
        ]
    )

    monkeypatch.setattr(
        "results.fetch.requests.get",
        lambda url, params=None, headers=None, timeout=None: next(responses),
    )
    monkeypatch.setattr("results.fetch.time_module.sleep", lambda _: None)

    actual = get_project_most_observed_species(265959)

    assert actual == {
        "media": {
            "url": "https://example.com/photo.jpg",
            "attribution": "Example",
            "original_dimensions": {"width": 100, "height": 80},
        },
        "scientific_name": "Danaus plexippus",
        "common_name": "Monarch",
        "count": 9,
    }


def test_get_project_identifiers_count_retries_on_rate_limit(monkeypatch):
    responses = iter(
        [
            _HttpResponseStub(
                status_code=429,
                payload={},
                headers={"Retry-After": "0"},
                text="normal_throttling",
            ),
            _HttpResponseStub(
                status_code=200,
                payload={"total_results": 44},
            ),
        ]
    )

    monkeypatch.setattr(
        "results.fetch.requests.get",
        lambda url, params=None, headers=None, timeout=None: next(responses),
    )
    monkeypatch.setattr("results.fetch.time_module.sleep", lambda _: None)

    assert get_project_identifiers_count(265959) == 44


def test_get_umbrella_project_stats_retries_on_rate_limit(monkeypatch):
    calls: list[dict] = []
    sleeps: list[float] = []
    responses = iter(
        [
            _HttpResponseStub(
                status_code=429,
                payload={},
                headers={"Retry-After": "0"},
                text="normal_throttling",
            ),
            _HttpResponseStub(
                status_code=200,
                payload={
                    "results": [
                        {
                            "observation_count": 12,
                            "species_count": 8,
                            "observers_count": 4,
                            "project": {"id": 123},
                        }
                    ]
                },
            ),
        ]
    )

    def fake_get(
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        timeout: float | None = None,
    ):
        calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return next(responses)

    monkeypatch.setattr("results.fetch.requests.get", fake_get)
    monkeypatch.setattr("results.fetch.time_module.sleep", sleeps.append)

    actual = get_umbrella_project_stats("city-nature-challenge-2026")

    assert len(calls) == 2
    assert sleeps == [5.0]
    assert actual == [
        {
            "observation_count": 12,
            "species_count": 8,
            "observers_count": 4,
            "project": {"id": 123},
        }
    ]


def test_get_non_inaturalist_project_stats_retries_on_rate_limit(monkeypatch):
    sleeps: list[float] = []

    class _SessionStub:
        def __init__(self) -> None:
            self._responses = iter(
                [
                    _HttpResponseStub(
                        status_code=429,
                        payload={},
                        headers={"Retry-After": "0"},
                        text="normal_throttling",
                    ),
                    _HttpResponseStub(
                        status_code=200,
                        payload={"observation_count": 7, "species_count": 3},
                    ),
                ]
            )
            self.calls: list[dict] = []

        def get(
            self,
            url: str,
            params: dict | None = None,
            headers: dict | None = None,
            timeout: float | None = None,
        ):
            self.calls.append(
                {
                    "url": url,
                    "params": params,
                    "headers": headers,
                    "timeout": timeout,
                }
            )
            return next(self._responses)

    session = _SessionStub()

    monkeypatch.setattr("results.fetch.time_module.sleep", sleeps.append)
    actual = get_non_inaturalist_project_stats(
        "https://example.com/non-inat.json",
        session=session,
    )

    assert len(session.calls) == 2
    assert sleeps == [5.0]
    assert actual == {"observation_count": 7, "species_count": 3}


def test_get_with_rate_limit_retry_uses_exponential_backoff_for_zero_retry_after(
    monkeypatch,
):
    sleeps: list[float] = []
    responses = iter(
        [
            _HttpResponseStub(
                status_code=429,
                payload={},
                headers={"Retry-After": "0"},
                text="normal_throttling",
            ),
            _HttpResponseStub(
                status_code=429,
                payload={},
                headers={"Retry-After": "0"},
                text="normal_throttling",
            ),
            _HttpResponseStub(
                status_code=200,
                payload={"results": []},
            ),
        ]
    )

    monkeypatch.setattr(
        "results.fetch.requests.get",
        lambda url, params=None, headers=None, timeout=None: next(responses),
    )
    monkeypatch.setattr("results.fetch.random.uniform", lambda _a, _b: 0.0)
    monkeypatch.setattr("results.fetch.time_module.sleep", sleeps.append)

    response = get_project_quality_grades(265959)

    assert sleeps == [5.0, 5.0]
    assert response == {"research": 0, "needs_id": 0, "casual": 0}


def test_get_with_rate_limit_retry_uses_prefect_backoff_config(monkeypatch):
    sleeps: list[float] = []
    responses = iter(
        [
            _HttpResponseStub(
                status_code=429,
                payload={},
                headers={"Retry-After": "0"},
                text="normal_throttling",
            ),
            _HttpResponseStub(
                status_code=200,
                payload={"results": []},
            ),
        ]
    )

    monkeypatch.setattr(
        "results.fetch.requests.get",
        lambda url, params=None, headers=None, timeout=None: next(responses),
    )
    monkeypatch.setattr("results.fetch.resolve_rate_limit_max_retries", lambda value=None: 9)
    monkeypatch.setattr(
        "results.fetch.resolve_rate_limit_backoff_factor",
        lambda value=None: 2.5,
    )
    monkeypatch.setattr(
        "results.fetch.resolve_rate_limit_min_retry_delay_seconds",
        lambda value=None: 7.0,
    )
    monkeypatch.setattr(
        "results.fetch.resolve_rate_limit_max_retry_delay_seconds",
        lambda value=None: 15.0,
    )
    monkeypatch.setattr("results.fetch.random.uniform", lambda _a, _b: 0.0)
    monkeypatch.setattr("results.fetch.time_module.sleep", sleeps.append)

    response = get_project_quality_grades(265959)

    assert sleeps == [7.0]
    assert response == {"research": 0, "needs_id": 0, "casual": 0}
