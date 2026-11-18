from datetime import UTC, datetime

import pytest

from streamlab.generator import GeneratorConfig


@pytest.fixture
def cfg() -> GeneratorConfig:
    return GeneratorConfig(
        seed=7,
        start=datetime(2026, 1, 1, tzinfo=UTC),
        duration_s=600,
        settle_s=120,
        duplicate_ratio=0.05,
        out_of_order_ratio=0.10,
        late_ratio=0.02,
        bad_ratio=0.01,
    )
