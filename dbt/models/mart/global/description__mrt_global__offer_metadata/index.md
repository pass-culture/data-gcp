# Table: Offer Metadata

The `mrt_global__offer_metadata` table is designed to store extensive information about offer metadata that is not directly provided in `Global Offer`. A lot of columns are only related to one cultural activity. There is also a lot of legacy columns that might change due to extensive work to improve the quality of our product metadata.

Offer metadata can either be manually provided by cultural partners upon offer creation, or automatically provided for synchronized offers. For synchronized offers, offer genres for books and music discs draw on the concept of Genre Tite Live (GTL). Titelive is a French company providing digital solutions for bookstores, publishers, and libraries to manage inventory, sales, and online catalogs. The concept of "Genre Tite Live" (GTL) refers to a classification system used by Titelive to categorize books and music cds based on their genre, to organize and manage collections more effectively. The GTL classification system consists of several hierarchical levels to categorize books and music cds, to provide a structured way to organize and retrieve products based on their content and audience.

## Table description

| name                 | data_type | description                                                                                                                     |
| -------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| offer_id             |           | Unique identifier for the offer.                                                                                                |
| offer_creation_date  |           | Date when the offer was created.                                                                                                |
| offer_subcategory_id |           | Identifier for the subcategory of the offer. Determined by the cultural partner via a list of pre-set options.                  |
| offer_category_id    |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu. |
| search_group_name    |           | Legacy: Category displayed in the application                                                                                   |
| offer_type_domain    |           | Deprecated: The offer's category type,as many metadata info are specific to certain                                             |
|                      |           | offer types. Can be either "BOOK", "MUSIC", "SHOW" or "MOVIE".                                                                  |
| offer_name           |           | Name of the offer as it appears in the application.                                                                             |
| offer_description    |           | Offer description (synopsis, further details on the show) as provided by thecultural partner and displayed in app.              |
| image_url            |           | Image displayed in the passculture.app if present.                                                                              |
| offer_type_id        |           | Deprecated: Unique identifier of the offer's type. Currently available to describe                                              |
|                      |           | either music genres (pop, rock) or show type (opera, circus).                                                                   |
| offer_sub_type_id    |           | Deprecated: Legacy identifier. Prefer using GTL.                                                                                |
| author               |           | The offer's author (a book's author, a music's singer, a movie's director).                                                     |
| performer            |           | Performers involved in this offer.                                                                                              |
| titelive_gtl_id      |           | Unique identifier of the Genre Tite Live (GTL) associated to this offer.                                                        |
| gtl_type             |           | Type of GTL associated to this offer. Can either be "BOOK" or "MUSIC"                                                           |
| gtl_label_level_1    |           | Name of the level 1 GTL associated to this offer (for example, "Littératurefor a                                                |
|                      |           | book or "Pop" for music.)                                                                                                       |
| gtl_label_level_2    |           | Name of the level 2 GTL associated to this offer (for example, "Poésie" for a book                                              |
|                      |           | or "Brit Pop" for music.)                                                                                                       |
| gtl_label_level_3    |           | Name of the level 3 GTL associated to this offer (for example, "Haiku" for a book).                                             |
|                      |           | Only available for books.                                                                                                       |
| gtl_label_level_4    |           | Name of the level 4 GTL associated to this offer. Only available for books.                                                     |
| offer_type_label     |           | Deprecated: Defines the offer genre for music, books and movies. Defines the offer                                              |
|                      |           | show type for shows.                                                                                                            |
| offer_type_labels    |           | Deprecated: Legacy identifier. Prefer using GTL.                                                                                |
| offer_sub_type_label |           | Deprecated: Defines the offer sub genre for music, books and movies. Defines the                                                |
|                      |           | offer show sub type for shows.                                                                                                  |
| offer_video_url      | STRING    | Indicates URL video of the offer downloaded by the cultural partner during offer creation.                                      |
