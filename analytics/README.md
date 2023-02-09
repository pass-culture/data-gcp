# Analytics

Repo contenant le code source pour les scripts python (ssh) pour la partie analytics du projet.

## Scripts 

### import_subcategories_model

- Recupère les sous_categories `api.src.pcapi.core.categories.subcategories_v2`
- Export `analytics_{env}.subcategories`
- Récupère les types d'offres :
    - show types `api.src.pcapi.domain.show_types`
    - music types `api.src.pcapi.domain.music_types`
    - movie types `api.src.pcapi.domain.movie_types`
    - book types `api.src.pcapi.domain.book_types`
- Export `analytics_{env}.offer_types`


### human_ids

Legacy script.
