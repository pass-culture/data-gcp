**artist_id**: Unique identifier of the artist.

**artist_cluster_id**: Internal identifier of artist names cluster.

**artist_wiki_id**: Wikidata ID (can be null if not matched).

**artist_name**: Name of the artist.

**normalized_artist_name**: Normalized name of the artist (lowercased, accents removed, tokens sorted alphabetically and joined with `|`), used for namesake deduplication.

**artist_offer_name**: Name of the artist as it is written in the offer.

**artist_description**: Short description of the artist.

**artist_biography**: Biography of the artist.

**artist_mediation_uuid**: Name of the image file related to the artist. Is a deterministic UUID based on the file URL.

**artist_computed_image**: URL of the most relevant image computed for the artist. It is the artist's own image if available (e.g. Wikidata image), otherwise it falls back to the image of the artist's most popular product (poster or recto mediation) based on bookings over the last 30 days.

**artist_wikipedia_url**: URL of the artist Wikipedia page.

**artist_type**: Whether the artist is a performer or an author.

**artist_creation_date**: Creation date of the artist.

**artist_modification_date**: Date of the last modification of the artist.

**artist_main_search_group_name**: Main `search_group_name` category of the artist (most offers, bookable or not). Restricted to LIVRES, CINEMA, MUSIQUE.

**artist_main_search_group_name_has_bookable_offer**: True if the artist has a bookable offer in their main_search_group_name.

**artist_associated_search_group_names**: Other `search_group_name` categories found among the artist's offers, besides the main one.

**artist_total_consultations**: Total consultations of offers related to the artist, after January 2025.

**artist_total_consultations_from_search**: Total consultations of offers related to the artist that came from search, after January 2025.

**artist_total_consultations_from_offer**: Total consultations of offers related to the artist that came from offer page, after January 2025.

**artist_total_consultations_from_venue**: Total consultations of offers related to the artist that came from venue page, after January 2025.

**artist_total_consultations_from_search_auto_complete**: Total consultations of offers related to the artist that came from search auto complete, after January 2025.

**artist_total_consulted_users**: Total users that consulted offers related to the artist, after January 2025.

**artist_total_products**: Total products created related to the artist.

**artist_total_offers**: Total offers created related to the artist.

**artist_total_bookable_offers**: Total bookable offers created related to the artist.

**artist_total_offer_categories**: Total distinct categories of offers created related to the artist.

**artist_total_venues**: Total venues that created offers related to the artist.

**artist_total_artist_types**: Total distinct type (author/performer) related to the artist in the base.

**artist_total_bookings**: Total bookings on offers related to the artist.

**artist_total_bookings_main_category_last_30d**: Bookings on the artist's offers over the last 30 days, in their main_search_group_name only. Used for the "trending" score.

**artist_total_offer_consultations_main_category_last_30d**: Offer-page consultations (ConsultOffer) on the artist's offers over the last 30 days, in their main_search_group_name only. Used for the "trending" score.

**artist_app_search_score**: Score of the artist computed by the DS team for app search.

**artist_pro_search_score**: Score of the artist computed by the DS team for pro search.
