---
title: QPI GTL Mapping
description: Description of the `ml_reco__qpi_gtl_mapping` table.
---

{% docs description__ml_reco__qpi_gtl_mapping %}

# Table: QPI GTL Mapping

The `ml_reco__qpi_gtl_mapping` table maps the UX category names shown in the QPI (Quiz de Préférences Initial) onboarding flow, along with their backend identifier (`backend_name`, e.g. "MANGAS"), to the resolved `gtl_id` from `int_applicative__titelive_gtl`. Each row corresponds to one `(ux_name, backend_name, gtl_id)` match: a `gtl_label` from the static QPI mapping can originate from any GTL level (1 to 4), so it is matched against all four levels, and only rows with an actual match are kept (unmatched labels and the "Aucun" placeholder categories are dropped).

{% enddocs %}
