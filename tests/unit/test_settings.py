from pathlib import Path

import pytest

from ephemeraldb.config.settings import load_settings


def test_settings_defaults() -> None:
    settings = load_settings({})

    assert settings.app_env == "local"
    assert settings.aws_region == "ap-south-1"
    assert settings.cache_dir == Path("/cache")
    assert settings.log_level == "INFO"
    assert settings.benchmark_seed == 42


def test_settings_environment_override() -> None:
    settings = load_settings(
        {
            "APP_ENV": "test",
            "AWS_REGION": "us-east-1",
            "CACHE_DIR": "/tmp/ephemeraldb",
            "LOG_LEVEL": "debug",
            "BENCHMARK_SEED": "123",
        }
    )

    assert settings.app_env == "test"
    assert settings.aws_region == "us-east-1"
    assert settings.cache_dir == Path("/tmp/ephemeraldb")
    assert settings.log_level == "DEBUG"
    assert settings.benchmark_seed == 123


def test_settings_rejects_invalid_integer() -> None:
    with pytest.raises(ValueError, match="BENCHMARK_SEED"):
        load_settings({"BENCHMARK_SEED": "not-an-int"})
