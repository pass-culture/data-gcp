## Scripts

### import_api_referentials

- Recupère les sous_categories depuis la route ̀`https://backend.passculture.app/native/v1/subcategories/v2`
  - :warning: Attention la route ne fournit pas toutes les informations donc on la merge avec un [précédent import](data/subcategories_v2_20250327.csv)
  - TODO: Quand cette route sera complète, on pourra supprimer le merge
- Export `raw_{env}.subcategories`
- Récupère les types d'offres :
    - show types `api.src.pcapi.domain.show_types`
    - music types `api.src.pcapi.domain.music_types`
    - movie types `api.src.pcapi.domain.movie_types`
    - book types `api.src.pcapi.domain.book_types`
- Export `raw_{env}.offer_types`
