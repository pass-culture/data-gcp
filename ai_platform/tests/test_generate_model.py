from pathlib import Path

from ai_platform.generate_model import generate_and_save_constant_model


def test_model(tmp_path):
    # Given
    model_path = Path(tmp_path) / "test_model.joblib"

    # When
    generate_and_save_constant_model(model_path)

    # Then
    assert model_path.is_file()
