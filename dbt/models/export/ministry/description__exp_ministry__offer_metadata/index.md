# Table: Offer Metadata

This model exports offer metadata for ministry use. It contains additional information about offers including:

- Basic metadata (search group name, author)
- Movie-specific information (movie ID, room ID, type, visa, release date, genres, countries)
- Book-specific information (editor, ISBN/EAN)
- GTL (Titelive Labels) classification hierarchy (4 levels)

## Table description

| name               | data_type | description                                                                         |
| ------------------ | --------- | ----------------------------------------------------------------------------------- |
| offer_id           |           | Unique identifier for the offer.                                                    |
| search_group_name  |           | Legacy: Category displayed in the application                                       |
| author             |           | The offer's author (a book's author, a music's singer, a movie's director).         |
| theater_movieId    |           | Allociné identifier for the movie, if applicable.                                   |
| theater_roomId     |           | Allociné identifier for the theater room, if applicable.                            |
| movie_type         |           | Type of movie, if applicable (e.g., feature film, short film).                      |
| movie_visa         |           | Film visa number, if applicable.                                                    |
| movie_release_date |           | Release date, if applicable.                                                        |
| movie_genres       |           | Genres of the film, if applicable.                                                  |
| movie_countries    |           | Countries where the film was produced, if applicable.                               |
| book_editor        |           | Editor of the book, if applicable.                                                  |
| offer_ean          |           | EAN of the offer, if applicable.                                                    |
| gtl_label_level_1  |           | Name of the level 1 GTL associated to this offer (for example, "Littératurefor a    |
|                    |           | book or "Pop" for music.)                                                           |
| gtl_label_level_2  |           | Name of the level 2 GTL associated to this offer (for example, "Poésie" for a book  |
|                    |           | or "Brit Pop" for music.)                                                           |
| gtl_label_level_3  |           | Name of the level 3 GTL associated to this offer (for example, "Haiku" for a book). |
|                    |           | Only available for books.                                                           |
| gtl_label_level_4  |           | Name of the level 4 GTL associated to this offer. Only available for books.         |
