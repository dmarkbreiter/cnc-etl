from typing import Any

from pydantic import SecretStr
from pydantic_settings import BaseSettings
from prefect.variables import Variable


class Settings(BaseSettings):
    do_spaces_key: SecretStr
    do_spaces_secret: SecretStr
    do_spaces_bucket: str = "cnc-assets"
    do_spaces_endpoint: str = "https://sfo3.digitaloceanspaces.com"
    do_spaces_region: str = "sfo3"
    year: int = 2026

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

PREFECT_YEAR_VARIABLE = "cnc_year"
PREFECT_IDENTIFIERS_COUNT_API_CALL_DELAY_VARIABLE = (
    "cnc_identifiers_count_api_call_delay"
)
PREFECT_QUALITY_GRADES_API_CALL_DELAY_VARIABLE = (
    "cnc_quality_grades_api_call_delay"
)
PREFECT_MOST_OBSERVED_SPECIES_API_CALL_DELAY_VARIABLE = (
    "cnc_most_observed_species_api_call_delay"
)
PREFECT_RATE_LIMIT_MAX_RETRIES_VARIABLE = "cnc_rate_limit_max_retries"
PREFECT_RATE_LIMIT_BACKOFF_FACTOR_VARIABLE = "cnc_rate_limit_backoff_factor"
PREFECT_RATE_LIMIT_MIN_RETRY_DELAY_SECONDS_VARIABLE = (
    "cnc_rate_limit_min_retry_delay_seconds"
)
PREFECT_RATE_LIMIT_MAX_RETRY_DELAY_SECONDS_VARIABLE = (
    "cnc_rate_limit_max_retry_delay_seconds"
)


def _get_prefect_variable(name: str, default: Any) -> Any:
    try:
        return Variable.get(name, default=default)
    except Exception:
        return default


def _resolve_float(value: object, fallback: float) -> float:
    if value is None:
        return fallback

    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _resolve_int(value: object, fallback: int) -> int:
    if value is None:
        return fallback

    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def resolve_year(year: int | None = None) -> int:
    if year is not None:
        return year

    variable_year = _get_prefect_variable(PREFECT_YEAR_VARIABLE, settings.year)
    if variable_year is None:
        return settings.year
    try:
        return int(variable_year)
    except (TypeError, ValueError):
        return settings.year


def resolve_identifiers_count_api_call_delay(
    api_call_delay: float | None = None,
    fallback: float = 3.0,
) -> float:
    if api_call_delay is not None:
        return api_call_delay

    variable_value = _get_prefect_variable(
        PREFECT_IDENTIFIERS_COUNT_API_CALL_DELAY_VARIABLE,
        fallback,
    )
    return _resolve_float(variable_value, fallback)


def resolve_quality_grades_api_call_delay(
    api_call_delay: float | None = None,
    fallback: float = 3.0,
) -> float:
    if api_call_delay is not None:
        return api_call_delay

    variable_value = _get_prefect_variable(
        PREFECT_QUALITY_GRADES_API_CALL_DELAY_VARIABLE,
        fallback,
    )
    return _resolve_float(variable_value, fallback)


def resolve_most_observed_species_api_call_delay(
    api_call_delay: float | None = None,
    fallback: float = 3.0,
) -> float:
    if api_call_delay is not None:
        return api_call_delay

    variable_value = _get_prefect_variable(
        PREFECT_MOST_OBSERVED_SPECIES_API_CALL_DELAY_VARIABLE,
        fallback,
    )
    return _resolve_float(variable_value, fallback)


def resolve_rate_limit_max_retries(max_retries: int | None = None, fallback: int = 5) -> int:
    if max_retries is not None:
        return max_retries

    variable_value = _get_prefect_variable(
        PREFECT_RATE_LIMIT_MAX_RETRIES_VARIABLE,
        fallback,
    )
    return max(0, _resolve_int(variable_value, fallback))


def resolve_rate_limit_backoff_factor(
    backoff_factor: float | None = None,
    fallback: float = 1.0,
) -> float:
    if backoff_factor is not None:
        return backoff_factor

    variable_value = _get_prefect_variable(
        PREFECT_RATE_LIMIT_BACKOFF_FACTOR_VARIABLE,
        fallback,
    )
    return max(0.0, _resolve_float(variable_value, fallback))


def resolve_rate_limit_min_retry_delay_seconds(
    min_retry_delay_seconds: float | None = None,
    fallback: float = 5.0,
) -> float:
    if min_retry_delay_seconds is not None:
        return min_retry_delay_seconds

    variable_value = _get_prefect_variable(
        PREFECT_RATE_LIMIT_MIN_RETRY_DELAY_SECONDS_VARIABLE,
        fallback,
    )
    return max(0.0, _resolve_float(variable_value, fallback))


def resolve_rate_limit_max_retry_delay_seconds(
    max_retry_delay_seconds: float | None = None,
    fallback: float = 60.0,
) -> float:
    if max_retry_delay_seconds is not None:
        return max_retry_delay_seconds

    variable_value = _get_prefect_variable(
        PREFECT_RATE_LIMIT_MAX_RETRY_DELAY_SECONDS_VARIABLE,
        fallback,
    )
    return max(0.0, _resolve_float(variable_value, fallback))
