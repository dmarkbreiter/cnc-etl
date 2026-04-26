from flows.update_umbrella_stats import count_totals


def test_count_totals_uses_requested_project_id(monkeypatch):
    called: dict[str, str] = {}

    def fake_get_umbrella_species_total(project_id: str) -> int:
        called["project_id"] = project_id
        return 42

    monkeypatch.setattr(
        "flows.update_umbrella_stats.get_umbrella_species_total",
        fake_get_umbrella_species_total,
    )

    actual = count_totals.fn(
        [
            {"observation_count": 12, "observers_count": 4},
            {"observation_count": 8, "observers_count": 3},
        ],
        "city-nature-challenge-2025",
    )

    assert called == {"project_id": "city-nature-challenge-2025"}
    assert actual == {
        "observation_count": 20,
        "species_count": 42,
        "observer_count": 7,
    }
