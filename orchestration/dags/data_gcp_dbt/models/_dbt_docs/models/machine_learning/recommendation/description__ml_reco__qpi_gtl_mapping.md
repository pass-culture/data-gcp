---
title: QPI GTL Mapping
description: Description of the `ml_reco__qpi_gtl_mapping` table.
---

{% docs description__ml_reco__qpi_gtl_mapping %}

# Table: QPI GTL Mapping

The `ml_reco__qpi_gtl_mapping` table maps the UX category names shown in the QPI (Quiz de Préférences Initial) onboarding flow, along with their backend identifier (`backend_name`, e.g. "MANGAS"), to the raw Titelive GTL labels (`gtl_label`) they cover. Each row corresponds to one `(ux_name, backend_name, gtl_label)` tuple: a `backend_name` can map to several `gtl_label`s (e.g. "CULTURE" covers 4 labels), and the "Aucun" placeholder categories surface with a null `gtl_label`. Downstream, `recommendation_v2` resolves `backend_name` to `gtl_label` via the `qpi_gtl_mapping_mv` materialized view to filter Vertex AI retrieval candidates on `gtl_l2`.

{% enddocs %}
