from datetime import time
import hashlib
import json
import random
import re
import time as time_module
from typing import Any, TypedDict, Optional

import requests
from settings import (
    resolve_rate_limit_backoff_factor,
    resolve_rate_limit_max_retries,
    resolve_rate_limit_max_retry_delay_seconds,
    resolve_rate_limit_min_retry_delay_seconds,
)


class MostObservedSpecies(TypedDict):
    media: dict[str, str]
    scientific_name: str
    common_name: str | None
    count: int


class QualityGradeCounts(TypedDict):
    research: int
    needs_id: int
    casual: int


class UmbrellaProjectStats(TypedDict):
    observation_count: int
    species_count: int
    observers_count: int
    project: dict


class NonInaturalistProjectStats(TypedDict, total=False):
    project: dict[str, Any]
    observation_count: int
    species_count: int
    observers_count: int
    identifiers_count: int
    research_grade_observations_count: int
    most_observed_species: list[dict[str, Any]]
    least_observed_species: list[dict[str, Any]]


REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/html,*/*",
}
OBSERVATION_ORG_BOT_CHALLENGE_MARKERS = (
    "Checking if you are not a bot",
    "/.within.website/x/xess/xess.min.css",
    "anubis",
)
ANUBIS_CHALLENGE_RE = re.compile(
    r'<script id="anubis_challenge" type="application/json">(?P<payload>.*?)</script>',
    re.S,
)


def _get_with_rate_limit_retry(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float | None = None,
    session: requests.Session | None = None,
    max_retries: int | None = None,
    backoff_factor: float | None = None,
    min_retry_delay_seconds: float | None = None,
    max_retry_delay_seconds: float | None = None,
) -> requests.Response:
    request = session.get if session is not None else requests.get
    resolved_max_retries = resolve_rate_limit_max_retries(max_retries)
    resolved_backoff_factor = resolve_rate_limit_backoff_factor(backoff_factor)
    resolved_min_retry_delay_seconds = resolve_rate_limit_min_retry_delay_seconds(
        min_retry_delay_seconds
    )
    resolved_max_retry_delay_seconds = resolve_rate_limit_max_retry_delay_seconds(
        max_retry_delay_seconds
    )
    if resolved_max_retry_delay_seconds < resolved_min_retry_delay_seconds:
        resolved_max_retry_delay_seconds = resolved_min_retry_delay_seconds

    for attempt in range(resolved_max_retries + 1):
        response = request(url, params=params, headers=headers, timeout=timeout)

        if response.status_code != 429:
            response.raise_for_status()
            return response

        if attempt == resolved_max_retries:
            response.raise_for_status()

        retry_after = response.headers.get("Retry-After")
        try:
            sleep_for = float(retry_after) if retry_after is not None else None
        except (TypeError, ValueError):
            sleep_for = None

        exponential_backoff = resolved_backoff_factor * (2**attempt) + random.uniform(
            0, 0.5
        )
        if sleep_for is None or sleep_for <= 0:
            sleep_for = exponential_backoff

        sleep_for = max(resolved_min_retry_delay_seconds, sleep_for)
        sleep_for = min(resolved_max_retry_delay_seconds, sleep_for)

        time_module.sleep(sleep_for)

    raise RuntimeError(f"Exceeded retry budget for {url}")


def _normalize_strapi_value(value):
    """
    Recursively unwrap Strapi `data` / `attributes` containers into plain values.
    """

    if isinstance(value, list):
        return [_normalize_strapi_value(item) for item in value]

    if isinstance(value, dict):
        if "data" in value and len(value) == 1:
            return _normalize_strapi_value(value["data"])

        if "attributes" in value and isinstance(value["attributes"], dict):
            normalized = _normalize_strapi_value(value["attributes"])
            if isinstance(normalized, dict) and "id" in value:
                normalized["id"] = value.get("id")
            return normalized

        return {key: _normalize_strapi_value(item) for key, item in value.items()}

    return value


def get_project_most_observed_species(project_id: int) -> MostObservedSpecies:
    """
    Get the most observed species for a given iNaturalist project.
     - project_id: iNaturalist project ID to query
    """
    response = _get_with_rate_limit_retry(
        "https://api.inaturalist.org/v2/observations/species_counts",
        params={
            "per_page": 1,
            "fields": "all",
            "project_id": project_id,
        },
        headers=REQUEST_HEADERS,
        timeout=30,
    )

    if response.status_code > 299:
        raise ValueError(
            f"Error fetching most observed species for project_id {project_id}: {response.status_code} {response.text}"
        )

    data = response.json()
    results = data.get("results", [])

    if not results:
        results.append(
            {
                "count": 0,
                "taxon": {"name": "", "preferred_common_name": ""},
                "media": {"url": "", "attribution": "", "original_dimensions": {}},
            }
        )

    taxon = results[0].get("taxon", {})

    return MostObservedSpecies(
        media={
            "url": taxon.get("default_photo", {}).get("medium_url", ""),
            "attribution": taxon.get("default_photo", {}).get("attribution", ""),
            "original_dimensions": taxon.get("default_photo", {}).get(
                "original_dimensions", {}
            ),
        },
        scientific_name=taxon.get("name", ""),
        common_name=taxon.get("preferred_common_name", "").capitalize(),
        count=results[0].get("count", 0),
    )


def get_project_identifiers_count(
    project_id: int,
) -> int:
    """
    Count of identifiers for a given iNaturalist project.
    """

    url = "https://api.inaturalist.org/v2/observations/identifiers"
    params = {"fields": "all", "project_id": project_id, "per_page": 1}

    # Request aggregated response from paginate (returns response-like dict)
    response = _get_with_rate_limit_retry(
        url,
        params=params,
        headers=REQUEST_HEADERS,
        timeout=30,
    )

    data = response.json()

    results_count = data.get("total_results", 0)
    return results_count


def get_project_quality_grades(project_id: int) -> QualityGradeCounts:
    """
    Get quality grade counts for a given iNaturalist project.
    """

    url = "https://api.inaturalist.org/v2/observations/quality_grades"
    params = {"fields": "all", "project_id": project_id, "per_page": 1}

    response = _get_with_rate_limit_retry(
        url,
        params=params,
        headers=REQUEST_HEADERS,
        timeout=30,
    )

    data = response.json()

    items = data.get("results", [])

    # Build a deterministic mapping with defaults for missing grades
    counts = {"research": 0, "needs_id": 0, "casual": 0}
    for item in items:
        grade = item.get("quality_grade")
        cnt = item.get("count", 0)
        if grade in counts:
            counts[grade] = cnt

    return QualityGradeCounts(**counts)


def get_umbrella_project_stats(project_id: str) -> list[UmbrellaProjectStats]:
    """
    Get aggregated stats for a given iNaturalist project.
    """

    response = _get_with_rate_limit_retry(
        "https://api.inaturalist.org/v2/observations/umbrella_project_stats",
        params={
            "project_id": project_id,
        },
        headers=REQUEST_HEADERS,
        timeout=30,
    )

    data = response.json()
    results = data.get("results", [])

    if not results:
        raise ValueError(f"No results found for project_id {project_id}")

    return results


def get_strapi_results(year: int) -> list[dict[str, Any]]:
    """
    Fetch raw project results for a City Nature Challenge year from Strapi.
    """

    response = _get_with_rate_limit_retry(
        "https://cnc.nhmlac.org/api/event-dates",
        params={
            "populate[0]": "results",
            "populate[results][populate][most_observed_species][populate]": "*",
            "filters[year][$eq]": year,
        },
        headers=REQUEST_HEADERS,
        timeout=30,
    )

    data = response.json()

    event_dates = _normalize_strapi_value(data.get("data", []))
    raw_results = []
    if event_dates:
        raw_results = event_dates[0].get("results", [])

    return [_normalize_strapi_value(result) for result in raw_results]


def _parse_json_response(
    response: requests.Response, *, endpoint: str | None = None
) -> dict[str, Any]:
    response.raise_for_status()
    response_text = response.text or ""

    if endpoint and "observation.org" in endpoint and all(
        marker in response_text for marker in OBSERVATION_ORG_BOT_CHALLENGE_MARKERS
    ):
        raise ValueError(
            f"Observation.org returned its bot-check HTML instead of JSON for {endpoint}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        snippet = response_text[:160].strip().replace("\n", " ")
        if endpoint:
            raise ValueError(
                f"Expected JSON response from {endpoint} but received: {snippet}"
            ) from exc
        raise ValueError(f"Expected JSON response but received: {snippet}") from exc

    if not isinstance(payload, dict):
        if endpoint:
            raise ValueError(
                f"Expected JSON object response from {endpoint}, received {type(payload)!r}"
            )
        raise ValueError(f"Expected JSON object response, received {type(payload)!r}")

    return payload


def _is_observation_org_bot_check(response_text: str, endpoint: str) -> bool:
    return "observation.org" in endpoint and all(
        marker in response_text for marker in OBSERVATION_ORG_BOT_CHALLENGE_MARKERS
    )


def _extract_anubis_challenge_payload(response_text: str) -> dict[str, Any]:
    match = ANUBIS_CHALLENGE_RE.search(response_text)
    if not match:
        raise ValueError("Observation.org bot-check page did not include a challenge payload")

    payload = json.loads(match.group("payload"))
    if not isinstance(payload, dict):
        raise ValueError("Observation.org challenge payload was not a JSON object")

    return payload


def _has_leading_zeroes(digest: bytes, difficulty: int) -> bool:
    full_bytes = difficulty // 2
    has_half_nibble = difficulty % 2 == 1

    if any(byte != 0 for byte in digest[:full_bytes]):
        return False

    if has_half_nibble and digest[full_bytes] >> 4 != 0:
        return False

    return True


def _solve_anubis_challenge(random_data: str, difficulty: int) -> tuple[int, str, int]:
    nonce = 0
    started_at = time_module.perf_counter()

    while True:
        digest = hashlib.sha256(f"{random_data}{nonce}".encode("utf-8")).digest()
        if _has_leading_zeroes(digest, difficulty):
            elapsed_ms = max(1, int((time_module.perf_counter() - started_at) * 1000))
            return nonce, digest.hex(), elapsed_ms
        nonce += 1


def _pass_observation_org_bot_check(
    client: requests.Session, endpoint: str, response_text: str
) -> dict[str, Any]:
    challenge_payload = _extract_anubis_challenge_payload(response_text)
    challenge = challenge_payload.get("challenge") or {}
    rules = challenge_payload.get("rules") or {}

    challenge_id = challenge.get("id")
    random_data = challenge.get("randomData")
    difficulty = int(rules.get("difficulty") or 0)

    if not challenge_id or not random_data or difficulty < 1:
        raise ValueError(
            f"Observation.org returned an incomplete bot-check challenge for {endpoint}"
        )

    nonce, challenge_response, elapsed_ms = _solve_anubis_challenge(
        random_data, difficulty
    )

    pass_challenge_url = requests.compat.urljoin(
        endpoint, "/.within.website/x/cmd/anubis/api/pass-challenge"
    )
    verification_response = client.get(
        pass_challenge_url,
        params={
            "id": challenge_id,
            "response": challenge_response,
            "nonce": nonce,
            "redir": endpoint,
            "elapsedTime": elapsed_ms,
        },
        headers=REQUEST_HEADERS,
        timeout=30,
        allow_redirects=False,
    )
    verification_response.raise_for_status()

    verified_response = client.get(endpoint, headers=REQUEST_HEADERS, timeout=30)
    return _parse_json_response(verified_response, endpoint=endpoint)


def get_non_inaturalist_project_stats(
    endpoint: str, *, session: requests.Session | None = None
) -> NonInaturalistProjectStats:
    client = session or requests.Session()
    response = _get_with_rate_limit_retry(
        endpoint,
        headers=REQUEST_HEADERS,
        timeout=30,
        session=client,
    )
    response_text = response.text or ""

    if _is_observation_org_bot_check(response_text, endpoint):
        return _pass_observation_org_bot_check(client, endpoint, response_text)

    return _parse_json_response(response, endpoint=endpoint)
