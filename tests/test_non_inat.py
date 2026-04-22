import hashlib

from flows.update_non_inat_stats import _normalize_non_inat_result
from results.fetch import _solve_anubis_challenge, get_non_inaturalist_project_stats


class _StubResponse:
    def __init__(
        self,
        *,
        payload: dict | None = None,
        text: str | None = None,
        status_code: int = 200,
    ):
        self._payload = payload
        self.text = text if text is not None else ""
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        if self._payload is None:
            raise ValueError("No JSON payload configured")
        return self._payload


class _StubSession:
    def __init__(self, responses: list[_StubResponse]):
        self._responses = responses
        self.calls = []

    def get(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self._responses.pop(0)


def test_get_non_inaturalist_project_stats_uses_direct_json_response():
    session = _StubSession(
        [
            _StubResponse(
                payload={
                    "project": {"id": 522, "title": "Barcelona"},
                    "observation_count": 12,
                    "species_count": 8,
                    "observers_count": 4,
                }
            )
        ]
    )

    actual = get_non_inaturalist_project_stats(
        "https://minka-sdg.org/cnc", session=session
    )

    assert actual == {
        "project": {"id": 522, "title": "Barcelona"},
        "observation_count": 12,
        "species_count": 8,
        "observers_count": 4,
    }
    assert [call["url"] for call in session.calls] == ["https://minka-sdg.org/cnc"]


def test_solve_anubis_challenge_returns_valid_pow_solution():
    nonce, response, elapsed_ms = _solve_anubis_challenge("seed", 4)

    assert elapsed_ms >= 1
    assert response == hashlib.sha256(f"seed{nonce}".encode("utf-8")).hexdigest()
    assert response.startswith("0000")


def test_get_non_inaturalist_project_stats_solves_observation_org_bot_check():
    session = _StubSession(
        [
            _StubResponse(
                text='<!doctype html><html lang="en"><head><title>Checking if you are not a bot</title><link rel="stylesheet" href="/.within.website/x/xess/xess.min.css?cachebuster=1"><script id="anubis_challenge" type="application/json">{"rules":{"difficulty":4},"challenge":{"id":"challenge-1","randomData":"seed"}}</script><script>anubis</script>'
            ),
            _StubResponse(text="", status_code=302),
            _StubResponse(
                payload={
                    "project": {"id": 1, "title": "Amsterdam"},
                    "observation_count": 7,
                    "species_count": 6,
                    "observers_count": 5,
                }
            ),
        ]
    )

    actual = get_non_inaturalist_project_stats(
        "https://observation.org/bioblitz/22303/cnc.json", session=session
    )

    assert actual == {
        "project": {"id": 1, "title": "Amsterdam"},
        "observation_count": 7,
        "species_count": 6,
        "observers_count": 5,
    }
    assert [call["url"] for call in session.calls] == [
        "https://observation.org/bioblitz/22303/cnc.json",
        "https://observation.org/.within.website/x/cmd/anubis/api/pass-challenge",
        "https://observation.org/bioblitz/22303/cnc.json",
    ]
    challenge_call = session.calls[1]
    assert challenge_call["params"]["id"] == "challenge-1"
    assert challenge_call["params"]["redir"] == (
        "https://observation.org/bioblitz/22303/cnc.json"
    )
    nonce = challenge_call["params"]["nonce"]
    response = challenge_call["params"]["response"]
    assert response == hashlib.sha256(f"seed{nonce}".encode("utf-8")).hexdigest()
    assert response.startswith("0000")


def test_normalize_non_inat_result_applies_defaults_for_missing_fields():
    actual = _normalize_non_inat_result(
        {
            "id": "3",
            "city": "Eindhoven",
            "country": "Netherlands",
            "link": "https://observation.org/bioblitz/22240/eindhoven-cnc-2026/",
            "endpoint": "https://observation.org/bioblitz/22240/cnc.json",
        },
        {
            "observation_count": 0,
            "species_count": 0,
            "observers_count": 0,
        },
    )

    assert actual == {
        "id": 3,
        "city": "Eindhoven",
        "most_observed_species": {
            "media": {
                "url": "",
                "attribution": "",
                "original_dimensions": {},
            },
            "scientific_name": "",
            "common_name": "",
            "count": 0,
        },
        "identifiers_count": 0,
        "quality_grades": {
            "research": 0,
            "needs_id": 0,
            "casual": 0,
        },
        "observation_count": 0,
        "species_count": 0,
        "observers_count": 0,
    }


def test_normalize_non_inat_result_maps_quality_grades_and_species_shape():
    actual = _normalize_non_inat_result(
        {
            "id": "522",
            "city": "Barcelona Metropolitan Area",
        },
        {
            "observation_count": 25,
            "species_count": 10,
            "observers_count": 5,
            "identifiers_count": 2,
            "research_grade_observations_count": 7,
            "most_observed_species": [
                {
                    "media": {
                        "url": "https://example.org/species.jpg",
                        "attribution": "CC-BY",
                        "original_dimensions": {"height": 100, "width": 200},
                    },
                    "scientific_name": "Corvus corax",
                    "common_name": "Common Raven",
                    "count": 3,
                }
            ],
        },
    )

    assert actual == {
        "id": 522,
        "city": "Barcelona Metropolitan Area",
        "most_observed_species": {
            "media": {
                "url": "https://example.org/species.jpg",
                "attribution": "CC-BY",
                "original_dimensions": {"height": 100, "width": 200},
            },
            "scientific_name": "Corvus corax",
            "common_name": "Common Raven",
            "count": 3,
        },
        "identifiers_count": 2,
        "quality_grades": {
            "research": 7,
            "needs_id": 0,
            "casual": 18,
        },
        "observation_count": 25,
        "species_count": 10,
        "observers_count": 5,
    }
