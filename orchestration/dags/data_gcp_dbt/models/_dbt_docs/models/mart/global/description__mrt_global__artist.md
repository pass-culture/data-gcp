---
title: Artist
description: Description of the `mrt_global__artist` table.
---

{% docs description__mrt_global__artist %}

# Table: Global Artist

The `mrt_global__artist` table is designed to store comprehensive information about artist representation on the application.

{% enddocs %}

The Product team and Data Science team have worked to relate offers to artists in order to link the offers related to the same artist and provide more relevant suggestions to users.

The artist_id is a home-made id that caracterizes artists with a particular artist_type and wikidatas related to these artists. Some artist_id can be related to different artist_types.

## Table description

This model agregates the use of the application by artist_id, in order to be able to analyse and rank them. It shows metrics related to consultation, booking and offers created by artist for example.

{% docs table__mrt_global__artist  %}{% enddocs %}
