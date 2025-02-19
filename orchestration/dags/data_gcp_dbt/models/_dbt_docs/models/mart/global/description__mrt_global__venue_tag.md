---
title: Venue Tag
description: Description of the `mrt_global__venue_tag` table.
---

{% docs description__mrt_global__venue_tag %}

The `mrt_global__venue_tag` table gathers all venue tags manually applied by our teams, in order to count and track the activity of cultural partners on a more granular level : by Ministry of Culture's labels, type of activity, playlist displayed on the Native App.


{% enddocs %}

Our teams developed theses tags because :
- product data and NAF code do not include these informations
or
- product data do share this information, but the declarative information is often erroneous

There are three types of venue tag (“venue_tag_category_label” fields) :
- "Comptage partenaire label et appellation du MC" : Ministry of Culture's labels, such as "Orchestre national", "Théâtre Lyrique", "Fonds régional d'art contemporain..'
- "Comptage partenaire sectoriel" : additional data on activity type of venues (CSTI, Education Populaire, MJC, Tiers-Lieu) which are not Ministry of Culture's labels
- "Playlist lieux et offres" : playlists which display the venue (/ offers of the venue) on the Native App. It enables editorial teams to measure the impact of display on venue activity (consultations, bookings..)

## Table description

{% docs table__mrt_global__venue_tag %}{% enddocs %}
