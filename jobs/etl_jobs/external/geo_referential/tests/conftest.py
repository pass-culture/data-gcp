from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def insee_like_xlsx() -> Path:
    return FIXTURES_DIR / "insee_like.xlsx"
