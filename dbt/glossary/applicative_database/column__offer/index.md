**offer_id**: Unique identifier for the offer.

**offer_product_id**: Identifier for the product associated with the offer.

**offer_product_humanized_id**: Human-readable identifier for the product associated with the offer.

**offer_id_at_providers**: Identifier for the offer at external providers.

**offer_is_synchronised**: Indicates whether the offer is synchronized with API systems and has a product_id.

**offer_description**: Offer description (synopsis, further details on the show) as provided by thecultural partner and displayed in app.

**offer_name**: Name of the offer as it appears in the application.

**offer_category_id**: Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu.

**offer_category**: Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu.

**offer_creation_date**: Date when the offer was created.

**offer_created_at**: Timestamp when the offer was created.

**offer_updated_at**: Timestamp when the offer was last updated.

**offer_is_duo**: Indicates if the offer can be booked as a duo.

**offer_is_underage_selectable**: Indicates if the offer is selectable for underage users.

**offer_reminder_id**: Unique identifier for the offer reminder associated with the offer.

# Metadata related

**offer_type**: Deprecated.

**offer_is_bookable**: Indicates if the offer is bookable.

An offer is considered bookable if it meets the following criteria:

1. **Stock availability**: The offer has non-expired and non-depleted stock.
1. **User access**: It can be booked by users in the app.
1. **Offerer Validation**: The offerer who created the offer must be officially validated.

**offer_is_digital_goods**: Indicates if the offer includes digital goods.

**offer_is_physical_goods**: Indicates if the offer includes physical goods.

**offer_is_event**: Indicates if the offer is an event.

**offer_humanized_id**: Human-readable identifier for the offer used in various platforms.

**passculture_pro_url**: URL to the offer on PassCulture Pro.

**webapp_url**: URL to the offer on the web application.

**offer_subcategory_id**: Identifier for the subcategory of the offer. Determined by the cultural partner via a list of pre-set options.

**offer_url**: URL to the offer.

**offer_is_national**: Indicates if the offer is available nationally. This Information is originally filled by pass Culture teams but is no longer used. It will be decommissioned soon.

**offer_is_numerical**: Indicates if the offer is digital (based on an url).

**offer_is_geolocated**: Indicates if the offer is geolocated.

**offer_is_active**: Indicates if the offer is active (the offer has been deactivated and is no longer visible in app).

**offer_validation**: Current status of the offer validation by internal teams. The offer may be:

- "DRAFT" (yet to be published by the cultural partner)
- "PENDING" (published, yet to be reviewed by the pass Culture Fraud team)
- "VALIDATED" (validated by pass Culture fraud team, ready to be made available to users)
- "REJECTED" (rejected and not made available to users)

**author**: The offer's author (a book's author, a music's singer, a movie's director).

**performer**: Performers involved in this offer.

**stage_director**: Stage director, if applicable.

**theater_movie_id**: Allociné identifier for the movie, if applicable.

**theater_room_id**: Allociné identifier for the theater room, if applicable.

**speaker**: Speaker or professor, if applicable.

**movie_type**: Type of movie, if applicable (e.g., feature film, short film).

**movie_visa**: Film visa number, if applicable.

**movie_release_date**: Release date, if applicable.

**movie_genres**: Genres of the film, if applicable.

**companies**: Companies involved in the production or distribution of the film, if applicable.

**movie_countries**: Countries where the film was produced, if applicable.

**casting**: Actors in the film, if applicable.

**isbn**: ISBN of the book, if applicable.

**offer_ean**: EAN of the offer, if applicable.

**rayon**: Literary genre, if applicable.

**offer_macro_rayon**: Semantic clustering of `column__rayon`.

**book_editor**: Editor of the book, if applicable.

**type**: Type of the offer.

**sub_type**: Sub-type of the offer.

**mediation_humanized_id**: Human-readable identifier for mediation.

**is_future_scheduled**: Indicates if the offer's publication is scheduled in the future.

**is_coming_soon**: Indicates if the offer is published in the app but not bookable yet (coming soon).

**item_id**: Identifier for the item associated with the offer used internally by the data science team.

**search_group_name**: Legacy: Category displayed in the application

**image_url**: Image displayed in the passculture.app if present.

**titelive_gtl_id**: Unique identifier of the Genre Tite Live (GTL) associated to this offer.

**gtl_type**: Type of GTL associated to this offer. Can either be "BOOK" or "MUSIC"

**gtl_label_level_1**: Name of the level 1 GTL associated to this offer (for example, "Littératurefor a book or "Pop" for music.)

**gtl_label_level_2**: Name of the level 2 GTL associated to this offer (for example, "Poésie" for a book or "Brit Pop" for music.)

**gtl_label_level_3**: Name of the level 3 GTL associated to this offer (for example, "Haiku" for a book). Only available for books.

**gtl_label_level_4**: Name of the level 4 GTL associated to this offer. Only available for books.

**offer_type_domain**: Deprecated: The offer's category type,as many metadata info are specific to certain offer types. Can be either "BOOK", "MUSIC", "SHOW" or "MOVIE".

**offer_type_id**: Deprecated: Unique identifier of the offer's type. Currently available to describe either music genres (pop, rock) or show type (opera, circus).

**offer_sub_type_id**: Deprecated: Legacy identifier. Prefer using GTL.

**offer_type_label**: Deprecated: Defines the offer genre for music, books and movies. Defines the offer show type for shows.

**offer_sub_type_label**: Deprecated: Defines the offer sub genre for music, books and movies. Defines the offer show sub type for shows.

**offer_type_labels**: Deprecated: Legacy identifier. Prefer using GTL.

**is_headlined**: Indicates if the offer is currently headlined on the app venue page.

**total_headlines**: Number of different times the offer was headlined on the app venue page. (an offer headline can be desactivated and reactivated)

**offer_last_provider_id**: Id of the offer's last provider.

**is_local_authority**: Indicates if the offerer is a local authority or not.

**offer_video_url**: Indicates URL video of the offer downloaded by the cultural partner during offer creation.

**offer_advice_content**: Indicates cultural partner recommendations for specific offers, accessible directly within the app.

**offer_has_mediation**: Indicates if the offer has a qualified mediation (cultural outreach).

**cultural_outreach_status**: Status of the cultural outreach: qualified, disqualified,pending or not applied if the checkbox was never checked

# Date related

**first_individual_offer_creation_date**: Date of the first individual offer creation.

**last_individual_offer_creation_date**: Date of the last individual offer creation.

**first_bookable_offer_date**: Date of the first bookable offer.

**last_bookable_offer_date**: Date of the last bookable offer.

**first_individual_bookable_offer_date**: Date of the first individual bookable offer.

**last_individual_bookable_offer_date**: Date of the last individual bookable offer.

**first_offer_creation_date**: Date of the first offer creation.

**last_offer_creation_date**: Date of the last offer creation.

**first_headline_date**: First date of headline on the app venue page.

**last_headline_date**: Last date of headline on the app venue page.

**offer_publication_date**: Publication date of the offer on the app, bookable or not (coming soon). Data available only from July 2025.

**offer_finalization_date**: Finalization date of the offer creation. If offer_creation_date is not null and offer_finalization_date is null, then it is draft. Data available only from July 2025.

**scheduled_offer_bookability_date**: Date of bookability of the offer. This field is filled only if the offer is scheduled.

**cultural_outreach_claimed_date**: Date at which the cultural outreach checkbox was checked
