# Table: Artist Score

The `exp_backend__artist_score` table contains the artist scores that must be synchronized with the backend application. It is an export from the ml_metadata\_\_artist_score model.

## Table description

| name                    | data_type | description                                                 |
| ----------------------- | --------- | ----------------------------------------------------------- |
| artist_id               |           | Unique identifier of the artist.                            |
| artist_app_search_score |           | Score of the artist computed by the DS team for app search. |
| artist_pro_search_score |           | Score of the artist computed by the DS team for pro search. |
