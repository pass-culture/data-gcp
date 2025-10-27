---
title: Product Delta
description: Daily delta table of products to be created or updated in the backend.
---

{% docs description__product_delta %}

# Table: Product Delta

The `product_delta` table contains the list of products (identified by EAN) and their parsed data that must be synchronized with the backend application.
It identifies whether a product is new (`add`) or needs to be updated (`update`) based on changes detected in the source snapshot since the last successful provider synchronization.

This model specifically filters for products where the `codesupport` contains alphabetic characters, to targeting physical books.

{% enddocs %}

## Key Concepts

- **Delta Generation**: This table is regenerated daily based on the `snapshot` history and the last successful synchronization timestamp recorded in `local_provider_event`.
- **Target Consumer**: Primarily used by the backend system to perform incremental updates (UPSERTs) on its product database.
- **Actions**:
    - `add`: The EAN does not exist in the backend's product table (according to specific criteria).
    - `update`: The EAN already exists and its data needs to be updated.

## Table Description

This section would typically display the auto-generated schema from dbt docs.

{% docs table__product_delta %}{% enddocs %}


---
## Column Descriptions

{% docs column__ean %}
**EAN (European Article Number)**: The unique identifier for the product. It serves as the primary key for joining and identifying products across systems.
{% enddocs %}

{% docs column__titre %}
**Title**: The title of the product (e.g., book title), extracted from the raw JSON payload.
{% enddocs %}

{% docs column__resume %}
**Summary**: The product's summary or description, extracted from `$.article[0].resume`. This field can be null.
{% enddocs %}

{% docs column__codesupport %}
**Support Code**: The code indicating the product's format (e.g., 'T', 'CD', 'P'). Extracted from `$.article[0].codesupport`.
{% enddocs %}

{% docs column__dateparution %}
**Publication Date**: The date the product was published, as a string. Extracted from `$.article[0].dateparution`. This field can be null.
{% enddocs %}

{% docs column__editeur %}
**Publisher**: The name of the publisher. Extracted from `$.article[0].editeur`. This field can be null.
{% enddocs %}

{% docs column__langueiso %}
**ISO Language Code**: The ISO code for the product's language (e.g., 'eng'). Extracted from `$.article[0].langueiso`.
{% enddocs %}

{% docs column__taux_tva %}
**VAT Rate**: The applicable VAT (TVA) rate for the product, as a string. Extracted from `$.article[0].taux_tva`.
{% enddocs %}

{% docs column__prix %}
**Price**: The product's price, cast to a NUMERIC type. Extracted from `$.article[0].prix`. This field can be null.
{% enddocs %}

{% docs column__id_lectorat %}
**Audience ID**: The identifier for the target audience (readership), cast to INT64. Extracted from `$.article[0].id_lectorat`.
{% enddocs %}

{% docs column__gtl %}
**GTL (Genre Titelive)**: A JSON object containing classification codes. Extracted from `$.article[0].gtl`.
{% enddocs %}

{% docs column__auteurs_multi %}
**Authors (Multiple)**: A JSON array containing information about the author(s). Extracted from `$.auteurs_multi`.
{% enddocs %}

{% docs column__recto_uuid %}
**Front Cover Image UUID**: The UUID of the front cover (recto) image associated with the product snapshot.
{% enddocs %}

{% docs column__verso_image_uuid %}
**Back Cover Image UUID**: The UUID of the back cover (verso) image associated with the product snapshot.
{% enddocs %}

{% docs column__product_modification_date %}
**Product Modification Date**: The timestamp (`dbt_valid_from`) indicating when this version of the product became active in the source snapshot. This helps trace when the change occurred.
{% enddocs %}

{% docs column__backend_crud_action %}
**Backend CRUD Action**: Specifies the operation the backend should perform for this EAN.
- `add`: Insert a new product record.
- `update`: Update an existing product record.
{% enddocs %}
