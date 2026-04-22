from results.fetch import get_strapi_results


class _StubResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_get_strapi_results_normalizes_strapi_payload(monkeypatch):
    calls = {}

    def fake_get(url: str, params: dict | None = None):
        calls["url"] = url
        calls["params"] = params
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
    def fake_get(url: str, params: dict | None = None):
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
