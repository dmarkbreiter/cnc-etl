import settings


def test_resolve_identifiers_count_api_call_delay_prefers_explicit_value(monkeypatch):
    monkeypatch.setattr(
        settings,
        "_get_prefect_variable",
        lambda name, default: 9.0,
    )

    assert settings.resolve_identifiers_count_api_call_delay(1.5) == 1.5


def test_resolve_identifiers_count_api_call_delay_uses_prefect_variable(monkeypatch):
    monkeypatch.setattr(
        settings,
        "_get_prefect_variable",
        lambda name, default: "4.25",
    )

    assert settings.resolve_identifiers_count_api_call_delay() == 4.25


def test_resolve_quality_grades_api_call_delay_falls_back_when_missing(monkeypatch):
    monkeypatch.setattr(
        settings,
        "_get_prefect_variable",
        lambda name, default: None,
    )

    assert settings.resolve_quality_grades_api_call_delay() == 3.0


def test_resolve_most_observed_species_api_call_delay_falls_back_when_invalid(
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "_get_prefect_variable",
        lambda name, default: "not-a-number",
    )

    assert settings.resolve_most_observed_species_api_call_delay() == 3.0


def test_resolve_rate_limit_max_retries_uses_prefect_variable(monkeypatch):
    monkeypatch.setattr(
        settings,
        "_get_prefect_variable",
        lambda name, default: "7",
    )

    assert settings.resolve_rate_limit_max_retries() == 7


def test_resolve_rate_limit_backoff_factor_falls_back_when_invalid(monkeypatch):
    monkeypatch.setattr(
        settings,
        "_get_prefect_variable",
        lambda name, default: "oops",
    )

    assert settings.resolve_rate_limit_backoff_factor() == 1.0


def test_resolve_rate_limit_min_retry_delay_seconds_uses_prefect_variable(monkeypatch):
    monkeypatch.setattr(
        settings,
        "_get_prefect_variable",
        lambda name, default: "8.5",
    )

    assert settings.resolve_rate_limit_min_retry_delay_seconds() == 8.5


def test_resolve_rate_limit_max_retry_delay_seconds_clamps_to_non_negative(
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "_get_prefect_variable",
        lambda name, default: "-1",
    )

    assert settings.resolve_rate_limit_max_retry_delay_seconds() == 0.0
