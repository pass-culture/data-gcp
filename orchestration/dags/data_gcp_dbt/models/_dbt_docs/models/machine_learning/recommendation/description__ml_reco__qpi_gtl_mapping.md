---
title: QPI GTL Mapping
description: Description of the `ml_reco__qpi_gtl_mapping` table.
---

{% docs description__ml_reco__qpi_gtl_mapping %}

# Table: QPI GTL Mapping

The `ml_reco__qpi_gtl_mapping` table maps the UX category names shown in the QPI (Quiz de Préférences Initial) onboarding flow, along with their backend identifier (`ux_code`, e.g. "MANGAS"), to the underlying Titelive GTL labels. Each row corresponds to one `(ux_name, gtl_label)` pair, enriched with the GTL type (`gtl_type`), the matched label (`gtl_label_matched`), the GTL hierarchy level at which it was matched (`matched_level`, 1 to 4) and the resolved `gtl_id` from `int_applicative__titelive_gtl`, since a `gtl_label` can originate from any GTL level.

{% enddocs %}
