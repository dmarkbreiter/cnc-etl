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
