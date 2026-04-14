from typing import Mapping, Any, Optional
import random
import time
import requests


def paginate(
    url: str,
    params: Optional[dict] = None,
    session: Optional[requests.Session] = None,
    per_page: int = 400,
    max_retries: int = 5,
    backoff_factor: float = 1.0,
) -> Mapping[str, Any]:
    """
    Iterate over paginated API responses.

    Parameters
    - url: endpoint to request
    - params: base query parameters (will be copied per-request)
    - session: optional `requests.Session` to reuse connection/config
    - page_param / per_page_param: query parameter names for paging
    - per_page: number of items per page to request
    - max_retries: how many 429 attempts to make before giving up
    - backoff_factor: base multiplier for exponential backoff when Retry-After
      isn't provided

    Behavior
    - Uses the provided `session` (or a fresh one) to make paged requests.
    - On HTTP 429 (Too Many Requests) it will first look for a `Retry-After`
      header and sleep for that duration. If absent, it falls back to
      exponential backoff with a small random jitter to avoid thundering-herd
      retry storms.
    - Raises for non-429 HTTP errors.
    - Yields individual items from the page's payload (tries `results`, then
      `data`). Stops when no items are returned or when `total_results`
      indicates the end.
    """

    # Use a session for connection pooling and shared configuration.
    sess = session or requests.Session()
    page = 1

    all_items: list[Any] = []
    total: int = 0
    last_meta: dict[str, Any] = {}

    while True:
        # Attempt loop handles transient 429 responses with backoff.
        attempt = 0
        while True:
            attempt += 1
            resp = sess.get(
                url,
                params={**(params or {}), "page": page, "per_page": per_page},
                timeout=10,
            )

            # If rate limited, honor Retry-After header when present.
            if resp.status_code == 429:
                if attempt > max_retries:
                    # Give up and surface the HTTPError to the caller.
                    resp.raise_for_status()

                retry_after = resp.headers.get("Retry-After")
                try:
                    # Retry-After can be a number of seconds or an HTTP date;
                    # try to parse numeric values first and fall back to
                    # exponential backoff when parsing fails.
                    sleep_for = float(retry_after) if retry_after is not None else None
                except (TypeError, ValueError):
                    sleep_for = None

                if sleep_for is None:
                    # Exponential backoff with jitter: base * 2^(attempt-1) + jitter
                    sleep_for = backoff_factor * (2 ** (attempt - 1)) + random.uniform(
                        0, 0.5
                    )

                time.sleep(sleep_for)
                # Retry the request after sleeping.
                continue

            # For any other status, raise an exception on error and proceed on success.
            resp.raise_for_status()
            break

        # Extract item list from the response payload.
        data = resp.json()
        last_meta = dict(data) if isinstance(data, dict) else {}
        items = data.get("results") or data.get("data") or []

        # If no items on this page, pagination complete.
        if not items:
            break

        # Append items from this page into the aggregated list.
        all_items.extend(items)

        # Update total if provided by the API.
        total = int(data.get("total_results", 0) or total or 0)

        # If the API reports a total item count, use it as a termination check.
        if total and len(all_items) >= total:
            break

        # Advance to the next page and repeat.
        page += 1

    # Construct a response-like dict similar to a single endpoint call.
    result: dict[str, Any] = dict(last_meta)
    result["results"] = all_items
    result["total_results"] = total or len(all_items)
    return result
