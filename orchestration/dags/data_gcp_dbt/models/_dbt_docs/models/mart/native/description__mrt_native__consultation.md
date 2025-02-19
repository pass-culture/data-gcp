---
title: Native Consultation
description: Description of the `mrt_native__consultation` table.
---

{% docs description__mrt_native__consultation %}

The `mrt_native__consultation` table provides detailed insights into user consultations within the native application. It captures user identifiers, session details, traffic origin and discovery scores, enabling analysis of user interactions and engagement with offers and venues. This table is essential for understanding user behavior and optimizing the user experience by analyzing the pathways through which users interact with the application.

{% enddocs %}

## Table description

Main focus of the table :
- It computes the discovery score linked to each consultation, which represents the ability of each user to consult diversified offers. Discovery score is the addition of 3 increments (0 or 1) : item (=1 if the user consults a new offer),category (=1 is the user consults an offer from a new category, which reflects a cultural sector. Exemple : book, cinema, live show..), subcategory (=1 is the user consults an offer from a new subcategory, which is more granular than categories. Exemple : audio book, cinema subscription, festival..)
- It provides more information on consultation origin : marketing campaigns linked, home datas (name, audience type..), consultation macro origin (main canal : search, home, similar_offer, deeplink..) and consultation micro origin (more granular information on the canal : type of home, type of research, origin of venue consultation which stems from offer consultation...)


{% docs table__mrt_native__consultation %}{% enddocs %}
