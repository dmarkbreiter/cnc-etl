from pydantic import SecretStr
from pydantic_settings import BaseSettings


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
