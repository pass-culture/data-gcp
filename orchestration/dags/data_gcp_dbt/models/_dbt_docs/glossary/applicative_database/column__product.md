{% docs column__product_id %} Unique identifier of the product. Offers point to it through `offer_product_id`. {% enddocs %}
{% docs column__product_ean %} EAN-13 of the product, unique across products; NULL for cinema products. Offers carry the same code in `isbn`. {% enddocs %}
{% docs column__product_name %} Title of the product as received from its provider (book, album or movie title). {% enddocs %}
{% docs column__product_description %} Summary of the product as received from its provider. {% enddocs %}
{% docs column__product_subcategory_id %} Subcategory of the product: `LIVRE_PAPIER`, `SUPPORT_PHYSIQUE_MUSIQUE_CD`, `SUPPORT_PHYSIQUE_MUSIQUE_VINYLE` or `SEANCE_CINE`. Offers linked to the product carry the same subcategory. {% enddocs %}
{% docs column__product_category_id %} Category of the product, derived from its subcategory (`LIVRE`, `MUSIQUE_ENREGISTREE`, `CINEMA`). {% enddocs %}
{% docs column__product_last_provider_id %} Identifier of the provider that last updated the product (Titelive for books and music, cinema providers for movies). {% enddocs %}
{% docs column__product_last_provider_name %} Name of the provider that last updated the product. {% enddocs %}
{% docs column__product_modified_at_last_provider_date %} Datetime of the product's last update by its provider. {% enddocs %}
{% docs column__product_has_image %} True when the product has at least one image (thumb). {% enddocs %}
{% docs column__product_author %} Author of the product (a book's author, an album's main artist), from the provider's metadata. {% enddocs %}
{% docs column__product_metadata_performer %} Performer of a music product, from the provider's metadata. {% enddocs %}
{% docs column__product_publisher %} Publisher (`editeur`) of a book or music product, from the provider's metadata. {% enddocs %}
{% docs column__product_music_label %} Record label of a music product, from the provider's metadata. {% enddocs %}
{% docs column__product_distributor %} Distributor of a book or music product, from the provider's metadata. {% enddocs %}
{% docs column__product_collection %} Collection a book belongs to, from the provider's metadata. {% enddocs %}
{% docs column__product_book_format %} Format of a book as sent by Titelive (for example `POCHE`, `REVUE`, `BEAUX LIVRES`); NULL when Titelive sends none. {% enddocs %}
{% docs column__product_language_iso %} ISO 639-2 language code of a book (for example `fra`, `eng`), from the provider's metadata. {% enddocs %}
{% docs column__product_clil_code %} CLIL code of a book (the French book trade's subject classification), from the provider's metadata. {% enddocs %}
{% docs column__product_rayon %} Titelive shelf (`rayon`) of a book. {% enddocs %}
{% docs column__product_release_date %} Release date of the product: publication date for books and music (`date_parution`, sent as `YYYY-MM-DD` or `DD/MM/YYYY`), release date for movies. NULL when the provider sends none. {% enddocs %}
{% docs column__product_price %} Public price in euros sent by the provider (`prix_livre` for books, `prix_musique` for music); not the price of any offer. {% enddocs %}
{% docs column__product_gtl_id %} Genre Tite Live (GTL) identifier of a book or music product, as sent by Titelive. Labels come from the GTL reference; an id absent from it leaves the labels NULL. {% enddocs %}
{% docs column__product_allocine_id %} Allociné identifier of a cinema product. {% enddocs %}
{% docs column__product_movie_visa %} Exploitation visa number of a movie (CNC). {% enddocs %}
{% docs column__product_movie_runtime %} Runtime of a movie, in minutes. {% enddocs %}
{% docs column__product_movie_production_year %} Production year of a movie. {% enddocs %}
{% docs column__product_total_offers %} Number of offers linked to the product (`offer_product_id`), whatever their status. {% enddocs %}
{% docs column__product_total_bookable_offers %} Number of offers linked to the product that are currently bookable. {% enddocs %}
{% docs column__product_total_venues %} Number of distinct venues with at least one offer linked to the product. {% enddocs %}
{% docs column__product_first_offer_creation_date %} Creation date of the first offer linked to the product. {% enddocs %}
{% docs column__product_total_individual_bookings %} Number of individual bookings on the offers linked to the product, cancelled bookings included. {% enddocs %}
{% docs column__product_total_non_cancelled_individual_bookings %} Number of individual bookings on the offers linked to the product, cancelled bookings excluded. {% enddocs %}
