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


def resolve_year(year: int | None = None) -> int:
    if year is not None:
        return year

    try:
        variable_year = Variable.get(PREFECT_YEAR_VARIABLE, default=settings.year)
        if variable_year is None:
            return settings.year
        return int(variable_year)
    except Exception:
        return settings.year
