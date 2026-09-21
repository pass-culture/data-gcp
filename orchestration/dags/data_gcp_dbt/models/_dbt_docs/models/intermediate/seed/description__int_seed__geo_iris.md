---
title: Geographic IRIS
description: Description of the `int_seed__geo_iris` table.
---

{% docs description__int_seed__geo_iris %}

# Table: Geographic IRIS

One row per IRIS (Îlots Regroupés pour l'Information Statistique), the reference geographic grain used to locate users, venues and institutions.

It joins the two tables of the geographic referential loaded by the `import_geo_referential` job:

- `raw.geo_iris`: IRIS code, label, type and geometry (IGN Contours IRIS for métropole, DROM, Saint-Pierre-et-Miquelon, Saint-Barthélemy and Saint-Martin; one pseudo-IRIS per municipality, type `Z`, code `<city_code>0000`, for Wallis-et-Futuna, Polynésie française and Nouvelle-Calédonie);
- `raw.geo_commune`: the municipal attributes of the IRIS' municipality — COG hierarchy (arrondissement, canton, territorial authority), EPCI (`ZZZZZZZZZ` / `Sans objet` when none), INSEE density grid, ZRR and FRR zonings. For the arrondissements of Paris, Lyon and Marseille these are the attributes of the parent municipality (`geo_code`), whose label is also used as `city_label`;

plus the `region_department` seed (department, region, academy and timezone labels, kept for reporting continuity) and the `rural_city_type_data` seed.

`density_macro_level` is `rural` for density levels 5 to 7. `iris_internal_id` is `md5(iris_code)` and is stable across vintages for an unchanged IRIS code.

{% enddocs %}

## Table description

{% docs table__int_seed__geo_iris %}{% enddocs %}
