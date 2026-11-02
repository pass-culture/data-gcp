---
title: QPI GTL Mapping
description: Description of the `ml_reco__qpi_gtl_mapping` table.
---

{% docs description__ml_reco__qpi_gtl_mapping %}

# Table: QPI GTL Mapping

The `ml_reco__qpi_gtl_mapping` table is a static reference table mapping the UX category names shown in the QPI (Quiz de Préférences Initial) onboarding flow to the underlying Titelive GTL level-1 labels (`gtl_label_level_1` in `int_applicative__titelive_gtl`), for both the LIVRE and MUSIQUE domains. Each UX name can group several GTL labels, exposed as an array.

{% enddocs %}
