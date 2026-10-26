!!! note
    This file is auto-generated

    Tracking that are not related to specific concepts for now

    :construction_worker_tone1: Work in progress :construction_worker_tone1:

{% docs column__click_type %}The type of click interaction by the user.{% enddocs %}
{% docs column__module_displayed_date %}The date when the module was displayed to the user.{% enddocs %}
{% docs column__module_displayed_timestamp %}The timestamp when the module was displayed to the user.{% enddocs %}
{% docs column__module_clicked_timestamp %}The timestamp when the module was clicked by the user.{% enddocs %}
{% docs column__consult_venue_timestamp %}The timestamp when the venue was consulted by the user.{% enddocs %}
{% docs column__consult_offer_timestamp %}The timestamp when the offer was consulted by the user.{% enddocs %}
{% docs column__fav_timestamp %}The timestamp when the entry was marked as favorite by the user.{% enddocs %}
{% docs column__booking_timestamp %}The timestamp when the booking was made by the user.{% enddocs %}

{% hide columns %}

{% docs column__home_audience %} The audience of the home, from its Contentful tag: '18', '15_17' or 'grand_public' (several values joined by ', ' when a home has more than one). Can be NULL. {% enddocs %}
{% docs column__user_lifecycle_home %} The user lifecycle stage targeted by the home, from its Contentful tag (e.g. 'pas_de_resa' for the home of users with no booking). NULL for homes without this tag. {% enddocs %}
{% docs column__home_type %} The type of home carrying the entry, from its Contentful tag: 'principale', 'n_1' (level N-1 home), 'pre_resa' (home of users with no booking) or 'evenement'. A module shared by several homes carries the values joined by ', ' (e.g. 'n_1, principale'). Can be NULL. {% enddocs %}
{% docs column__parent_home_type %}The type of home associated with the parent entry.{% enddocs %}
{% docs column__playlist_type %}The type of playlist associated with the entry.{% enddocs %}
{% docs column__playlist_reach %}The reach of the playlist associated with the entry.{% enddocs %}
{% docs column__playlist_recurrence %}The recurrence pattern of the playlist.{% enddocs %}
{% docs column__displayed_position %}The position at which the offer was displayed on the home screen.{% enddocs %}
{% docs column__offer_display_order %}The display order (0-based index) of the offer within its recommendation playlist.{% enddocs %}

{% endhide %}
