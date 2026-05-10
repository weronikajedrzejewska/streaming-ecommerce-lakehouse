from pathlib import Path

from src.streaming.stream_to_bronze_silver import ensure_parent_dirs


def test_ensure_parent_dirs_creates_expected_directories(tmp_path: Path) -> None:
    bronze = tmp_path / "data" / "bronze" / "events"
    silver = tmp_path / "data" / "silver" / "events"
    metrics = tmp_path / "data" / "silver" / "metrics"

    ensure_parent_dirs(str(bronze), str(silver), str(metrics))

    assert bronze.exists()
    assert silver.exists()
    assert metrics.exists()
