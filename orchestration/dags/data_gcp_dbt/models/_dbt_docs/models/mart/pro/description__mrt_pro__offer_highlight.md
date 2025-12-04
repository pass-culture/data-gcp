---
title: Pro offer highlight
description: Description of the `mrt_pro__offer_highlight` table.
---

{% docs description__mrt_pro__offer_highlight %}

The `mrt_pro__offer_highlight` table gathers all participation requests submitted by cultural partners for promotional campaigns featured on the app ("Temps de valorisations thématiques" = highlights).
The Programmation team can then validate or not each highlight request.
The table also provides with information about the different highlights, including visual reference (image ID), the partner application window, and the dates of active display on the app.

{% enddocs %}

## Table description

Rules :
- Highlight_request_id is the key of the table
- A single offer can apply several times to a highlight, or to several highlights.
- When a highlight request is validated by the Programmation Team, the associated offer appears in the global_offer_criterion table (criterion_category_label = "Temps de valorisations thématiques") with the right highlight_id.
