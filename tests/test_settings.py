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
