---
title: Product Delta
description: Daily delta table of products to be created or updated in the backend.
---

{% docs description__product_delta %}

# Table: Product Delta

The `product_delta` table contains the list of products (identified by EAN) and their parsed data that must be synchronized with the backend application.

This model specifically filters for products where the `support code` contains alphabetic characters, to targeting physical books.

{% enddocs %}

## Key Concepts

- **Delta Generation**: This table is regenerated daily based on the `snapshot` history and the last successful synchronization timestamp recorded in `local_provider_event`.
- **Target Consumer**: Primarily used by the backend system to perform incremental updates (UPSERTs) on its product database.

## Table Description

This section would typically display the auto-generated schema from dbt docs.

{% docs table__product_delta %}{% enddocs %}

---

## Column Descriptions

{% docs column__ean %}
**EAN (European Article Number)**: The unique identifier for the product. It serves as the primary key for joining and identifying products across systems.
{% enddocs %}

{% docs column__title %}
**Title**: The title of the product (e.g., book title), extracted from the raw JSON payload.
{% enddocs %}

{% docs column__description %}
**Summary**: The product's summary or description, extracted from `$.article[0].resume`. This field can be null.
{% enddocs %}

{% docs column__support_code %}
**Support Code**: The code indicating the product's format (e.g., 'T', 'CD', 'P'). Extracted from `$.article[0].codesupport`.
{% enddocs %}

{% docs column__publication_date %}
**Publication Date**: The date the product was published, as a string. Extracted from `$.article[0].dateparution`. This field can be null.
{% enddocs %}

{% docs column__publisher %}
**Publisher**: The name of the publisher. Extracted from `$.article[0].editeur`. This field can be null.
{% enddocs %}

{% docs column__language_iso %}
**ISO Language Code**: The ISO code for the product's language (e.g., 'eng'). Extracted from `$.article[0].langueiso`. This field can be null.
{% enddocs %}

{% docs column__vat_rate %}
**VAT Rate**: The applicable VAT (TVA) rate for the product, as a string. Extracted from `$.article[0].taux_tva`. This field can be null.
{% enddocs %}

{% docs column__price %}
**Price**: The product's price, cast to a NUMERIC type. Extracted from `$.article[0].prix`. This field can be null.
{% enddocs %}

{% docs column__readership_id %}
**Readership ID**: The identifier for the target audience (readership), cast to INT64. Extracted from `$.article[0].id_lectorat`. This field can be null.
{% enddocs %}

{% docs column__gtl %}
**GTL (Genre Titelive)**: A JSON object containing classification codes. Extracted from `$.article[0].gtl`.
{% enddocs %}

{% docs column__multiple_authors %}
**Authors (Multiple)**: A JSON string/array/object containing information about the author(s). Extracted from `$.auteurs_multi`.
{% enddocs %}

{% docs column__recto_uuid %}
**Front Cover Image UUID**: The UUID of the front cover (recto) image associated with the product snapshot.
{% enddocs %}

{% docs column__verso_uuid %}
**Back Cover Image UUID**: The UUID of the back cover (verso) image associated with the product snapshot.
{% enddocs %}

{% docs column__image %}
**Front Cover Flag**: An integer (e.g., 0 or 1) indicating if a front cover image is available. Extracted from `$.article[0].image`.
{% enddocs %}

{% docs column__image_4 %}
**Back Cover Flag**: An integer (e.g., 0 or 1) indicating if a back cover image is available. Extracted from `$.article[0].image_4`.
{% enddocs %}

{% docs column__modification_date %}
**Modification Date**: The timestamp (`dbt_valid_from`) indicating when this version of the product became active in the source snapshot. This helps trace when the change occurred.
{% enddocs %}

{% docs column__product_type %}
**Product Type**: Indicating the type of product. Values can be `'paper'` (for books) or `'music'` (for CDs/Vinyls), determined by the format of the support code.
{% enddocs %}

{% docs column__artist %}
**Artist**: The main artist or band name. Extracted from `$.article[0].artiste`.
{% enddocs %}

{% docs column__music_label %}
**Music Label**: The production label. Extracted from `$.article[0].label`.
{% enddocs %}

{% docs column__composer %}
**Composer**: The composer's name. Extracted from `$.article[0].compositeur`.
{% enddocs %}

{% docs column__product_performer %}
**Performer**: The performer or interpreter. Extracted from `$.article[0].interprete`.
{% enddocs %}

{% docs column__nb_discs %}
**Number of Discs**: The number of physical discs/vinyls included. Extracted from `$.article[0].nb_galettes`.
{% enddocs %}
