{% docs column__offer_id %} Unique identifier for the offer. {% enddocs %}
{% docs column__offer_product_id %} Identifier for the product associated with the offer. {% enddocs %}
{% docs column__offer_product_humanized_id %} Human-readable identifier for the product associated with the offer. {%
enddocs %}
{% docs column__offer_id_at_providers %} Identifier for the offer at external providers. {% enddocs %}
{% docs column__offer_is_synchronised %} Indicates whether the offer is synchronized with API systems and has a
product_id. {% enddocs %}
{% docs column__offer_description %} Offer description (synopsis, further details on the show) as provided by the
cultural partner and displayed in app.{% enddocs %}

{% docs column__offer_name %} Name of the offer as it appears in the application. {% enddocs %}
{% docs column__offer_category_id %} Identifier for the category of the offer. {% enddocs %}
{% docs column__offer_creation_date %} Date when the offer was created. {% enddocs %}
{% docs column__offer_created_at %} Timestamp when the offer was created. {% enddocs %}
{% docs column__offer_updated_date %} Timestamp when the offer was last updated. {% enddocs %}
{% docs column__offer_is_duo %} Indicates if the offer can be booked as a duo. {% enddocs %}
{% docs column__offer_is_underage_selectable %} Indicates if the offer is selectable for underage users. {% enddocs %}

# Metadata related

{% docs column__offer_type %} Deprecated. {% enddocs %}
{% docs column__offer_is_bookable %} Indicates if the offer is bookable. {% enddocs %}
{% docs column__digital_goods %} Indicates if the offer includes digital goods. {% enddocs %}
{% docs column__physical_goods %} Indicates if the offer includes physical goods. {% enddocs %}
{% docs column__event %} Indicates if the offer is an event. {% enddocs %}
{% docs column__offer_humanized_id %} Human-readable identifier for the offer used in various platforms. {% enddocs %}
{% docs column__passculture_pro_url %} URL to the offer on PassCulture Pro. {% enddocs %}
{% docs column__webapp_url %} URL to the offer on the web application. {% enddocs %}
{% docs column__offer_subcategory_id %} Identifier for the subcategory of the offer. {% enddocs %}
{% docs column__offer_url %} URL to the offer. {% enddocs %}
{% docs column__is_national %} Indicates if the offer is available nationally. {% enddocs %}
{% docs column__offer_is_numerical %} Indicates if the offer is digital (based on an url). {% enddocs %}
{% docs column__offer_is_geolocated %} Indicates if the offer is geolocated. {% enddocs %}
{% docs column__is_active %} Indicates if the offer is active. {% enddocs %}
{% docs column__offer_validation %} Validation status of the offer. {% enddocs %}
{% docs column__author %} The offer's author (a book's author, a music's singer, a movie's director).{% enddocs %}
{% docs column__performer %} Performers involved in this offer.{% enddocs %}
{% docs column__stage_director %} Stage director, if applicable. {% enddocs %}
{% docs column__theater_movie_id %} Allociné identifier for the movie, if applicable. {% enddocs %}
{% docs column__theater_room_id %} Allociné identifier for the theater room, if applicable. {% enddocs %}
{% docs column__speaker %} Speaker or professor, if applicable. {% enddocs %}
{% docs column__movie_type %} Type of movie, if applicable (e.g., feature film, short film). {% enddocs %}
{% docs column__visa %} Film visa number, if applicable. {% enddocs %}
{% docs column__release_date %} Release date, if applicable. {% enddocs %}
{% docs column__genres %} Genres of the film, if applicable. {% enddocs %}
{% docs column__companies %} Companies involved in the production or distribution of the film, if applicable. {% enddocs
%}
{% docs column__countries %} Countries where the film was produced, if applicable. {% enddocs %}
{% docs column__casting %} Actors in the film, if applicable. {% enddocs %}
{% docs column__isbn %} ISBN of the book, if applicable. {% enddocs %}
{% docs column__rayon %} Literary genre, if applicable. {% enddocs %}
{% docs column__offer_macro_rayon %} Semantic clustering of `column__rayon`. {% enddocs %}
{% docs column__book_editor %} Editor of the book, if applicable. {% enddocs %}
{% docs column__type %} Type of the offer. {% enddocs %}
{% docs column__sub_type %} Sub-type of the offer. {% enddocs %}
{% docs column__mediation_humanized_id %} Human-readable identifier for mediation. {% enddocs %}
{% docs column__offer_publication_date %} Publication date of the offer. {% enddocs %}
{% docs column__is_future_scheduled %} Indicates if the offer is scheduled for the future. {% enddocs %}
{% docs column__item_id %}Identifier for the item associated with the offer used internally by the data science team. {%
enddocs %}
{% docs column__search_group_name %} Legacy: Category displayed in the application {% enddocs %}
{% docs column__image_url %} Image displayed in the passculture.app if present. {% enddocs %}
{% docs column__titelive_gtl_id %} Unique identifier of the Genre Tite Live (GTL) associated to this offer.{% enddocs %}
{% docs column__gtl_type %} Type of GTL associated to this offer. Can either be "BOOK" or "MUSIC" {% enddocs %}
{% docs column__gtl_label_level_1 %} Name of the level 1 GTL associated to this offer (for example, "Littératurefor a
book or "Pop" for music.) {% enddocs %}
{% docs column__gtl_label_level_2 %} Name of the level 2 GTL associated to this offer (for example, "Poésie" for a book
or "Brit Pop" for music.) {% enddocs %}
{% docs column__gtl_label_level_3 %} Name of the level 3 GTL associated to this offer (for example, "Haiku" for a book).
Only available for books. {% enddocs %}
{% docs column__gtl_label_level_4 %} Name of the level 4 GTL associated to this offer. Only available for books. {%
enddocs %}
{% docs column__offer_type_domain %} Deprecated: The offer's category type,as many metadata info are specific to certain
offer types. Can be either "BOOK", "MUSIC", "SHOW" or "MOVIE".{% enddocs %}
{% docs column__offer_type_id %} Deprecated: Unique identifier of the offer's type. Currently available to describe
either music genres (pop, rock) or show type (opera, circus).{% enddocs %}
{% docs column__offer_sub_type_id %} Deprecated: Legacy identifier. Prefer using GTL. {% enddocs %}
{% docs column__offer_type_label %} Deprecated: Defines the offer genre for music, books and movies. Defines the offer
show type for shows. {% enddocs %}
{% docs column__offer_sub_type_label %} Deprecated: Defines the offer sub genre for music, books and movies. Defines the
offer show sub type for shows. {% enddocs %}
{% docs column__offer_type_labels %} Deprecated: Legacy identifier. Prefer using GTL. {% enddocs %}

# Date related

{% docs column__first_individual_offer_creation_date %} Date of the first individual offer creation. {% enddocs %}
{% docs column__last_individual_offer_creation_date %} Date of the last individual offer creation. {% enddocs %}
{% docs column__first_bookable_offer_date %} Date of the first bookable offer. {% enddocs %}
{% docs column__last_bookable_offer_date %} Date of the last bookable offer. {% enddocs %}
{% docs column__first_individual_bookable_offer_date %} Date of the first individual bookable offer. {% enddocs %}
{% docs column__last_individual_bookable_offer_date %} Date of the last individual bookable offer. {% enddocs %}
{% docs column__first_offer_creation_date %} Date of the first offer creation. {% enddocs %}
{% docs column__last_offer_creation_date %} Date of the last offer creation. {% enddocs %}
