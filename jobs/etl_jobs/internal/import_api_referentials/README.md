## Scripts 

### import_api_referentials

- Recupère les sous_categories `api.src.pcapi.core.categories.subcategories_v2`
- Export `analytics_{env}.subcategories`
- Récupère les types d'offres :
    - show types `api.src.pcapi.domain.show_types`
    - music types `api.src.pcapi.domain.music_types`
    - movie types `api.src.pcapi.domain.movie_types`
    - book types `api.src.pcapi.domain.book_types`
- Export `analytics_{env}.offer_types`