---
title: Student EAC coverage KPI
description: Description of the `int_kpi__student_coverage` table.
---

{% docs description__int_kpi__student_coverage %}
KPI table aggregating EAC student coverage by month, school year, and territory geography (department / region / academy).

Each row represents a `partition_month` / `scholar_year` / department combination.
The model is built from [`adage_involved_student`](#!/model/model.data_gcp_dbt.adage_involved_student)
and takes the snapshot of the **last day of each month** per school year.

It exposes:
- `total_eligible_students`: total number of students eligible for EAC in the territory
- `total_eac_students`: total number of students who actually participated in at least one EAC activity

{% enddocs %}
