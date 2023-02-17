import pytest
from pcreco.models.reco.parser import underscore_to_camel


class PlaylistParamsTest:
    @pytest.mark.parametrize(
        ["input_str", "output_str"],
        [
            ("snake_case", "snakeCase"),
            ("camel_case", "camelCase"),
        ],
    )
    def test_underscore_to_camel(self, input_str, output_str):
        assert underscore_to_camel(input_str) == output_str
