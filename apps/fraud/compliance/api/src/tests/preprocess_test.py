import pytest
from pcpapillon.core.preprocess import prepare_features


class PreprocessTest:
    @pytest.mark.parametrize(
        ["input_body", "expected_preprocess"],
        [
            (
                {
                    "offer_id": "420",
                    "offer_name": "ninho 4 albums studio + poster",
                    "offer_description": "ninho 4 albums studio + poster",
                    "offer_subcategoryid": "SUPPORT_PHYSIQUE_MUSIQUE",
                    "rayon": None,
                    "macro_rayon": "",
                    "stock_price": "20",
                    "stock": "3",
                    "offer_image": "https://storage.googleapis.com/passculture-metier-prod-production-assets-fine-grained/thumbs/products/9YKMS",
                },
                {
                    "offer_id": "420",
                    "offer_name": "ninho 4 albums studio + poster",
                    "offer_description": "ninho 4 albums studio + poster",
                    "offer_subcategoryid": "SUPPORT_PHYSIQUE_MUSIQUE",
                    "rayon": "",
                    "macro_rayon": "",
                    "stock_price": 20,
                    "stock": 3,
                    "offer_image": "https://storage.googleapis.com/passculture-metier-prod-production-assets-fine-grained/thumbs/products/9YKMS",
                },
            )
        ],
    )
    def test_underscore_to_camel(self, input_body, expected_preprocess):
        assert prepare_features(input_body) == expected_preprocess
