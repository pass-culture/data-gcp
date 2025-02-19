!!! note
    This file is auto-generated

    :construction_worker_tone1: Work in progress :construction_worker_tone1:

{% docs column__consultation_id %}The unique identifier for each consultation event.{% enddocs %}
{% docs column__consultation_date %}The date on which the consultation took place.{% enddocs %}
{% docs column__consultation_macro_origin %}The macro origin of the consultation, indicating the broader context of its initiation : search, home, similar_offer, deeplink..{% enddocs %}
{% docs column__consultation_micro_origin %}The micro origin of the consultation, which provides more granular information on the canal : type of home, type of research, origin of venue consultation which stems from offer consultation...{% enddocs %}



{% hide columns %}


!!! note
    TODO: Rename fields of create a glossary concept about discovery

{% docs column__item_discovery_score %}The discovery score increment related to the discovery of a new item_id. =1 if the user consults a new offer, 0 if not.{% enddocs %}
{% docs column__subcategory_discovery_score %}The discovery score increment related to the discovery of a new subcategory. =1 if the user consults an offer from a new subcategory, which is more granular than categories. Exemple : audio book, cinema subscription, festival.. {% enddocs %}
{% docs column__category_discovery_score %}The discovery score increment related to the discovery of a new category. =1 if the user consults an offer from a new category, which reflects a cultural sector. Exemple : book, cinema, live show..){% enddocs %}
{% docs column__discovery_score %}The total discovery score. It is the addition of item_discovery_score, subcategory_discovery_score and category_discovery_score{% enddocs %}
{% docs column__is_category_discovered %}A boolean indicating if a category was discovered during the consultation.{% enddocs %}
{% docs column__is_subcategory_discovered %}A boolean indicating if a subcategory was discovered during the consultation.{% enddocs %}

{% endhide %}
