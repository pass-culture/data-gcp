{% docs column__institution_id %} Id of the institution. {% enddocs %}
{% docs column__institution_external_id %}The external identifier for the educational institution.{% enddocs %}
{% docs column__institution_program_name %} Name of the program at the educational institution. {% enddocs %}
{% docs column__institution_internal_iris_id %} Internal IRIS identifier for the educational institution. {% enddocs %}
{% docs column__institution_name %} Name of the educational institution. {% enddocs %}
{% docs column__institution_academy_name %} Academy name of the educational institution. {% enddocs %}
{% docs column__institution_region_name %} Region name of the educational institution. {% enddocs %}
{% docs column__institution_region_code %} INSEE region code of the educational institution, derived from the department code via the region_department seed. Defaults to '-1' when not found. {% enddocs %}
{% docs column__institution_department_code %} Department code of the educational institution. {% enddocs %}
{% docs column__institution_department_name %} Department name of the educational institution. {% enddocs %}
{% docs column__institution_postal_code %} Postal code of the educational institution. {% enddocs %}
{% docs column__institution_city %} Name of the city where the educational institution is located, at the arrondissement level for Paris, Lyon and Marseille (e.g. `Paris 1er Arrondissement`). {% enddocs %}
{% docs column__institution_city_code %} INSEE code of the city where the educational institution is located, at the arrondissement level for Paris, Lyon and Marseille (e.g. `75101`). {% enddocs %}
{% docs column__institution_municipality_code %} INSEE code of the municipality (commune) where the educational institution is located: the parent municipality for the arrondissements of Paris, Lyon and Marseille (`75056`, `69123`, `13055`), the same as `institution_city_code` everywhere else. {% enddocs %}
{% docs column__institution_municipality_label %} Name of the municipality (commune) of `institution_municipality_code` (e.g. `Paris` for every Paris arrondissement). {% enddocs %}
{% docs column__institution_epci %} EPCI name of the educational institution. {% enddocs %}
{% docs column__institution_epci_code %} EPCI code of the educational institution. {% enddocs %}
{% docs column__institution_density_label %} Density label of the educational institution's area. {% enddocs %}
{% docs column__institution_macro_density_label %} Macro density label of the educational institution : rural or urban. {% enddocs %}
{% docs column__institution_density_level %} Density level of the educational institution's area. {% enddocs %}
{% docs column__institution_type %} Type of institution. {% enddocs %}
{% docs column__macro_institution_type %} Type of macro institution. Values can be : COLLEGE; ECOLE; LYCEE. {% enddocs %}
{% docs column__institution_in_qpv %}Indicates whether the educational institution is located in a QPV or not(Priority Urban Area).{% enddocs %}
{% docs column__institution_latitude %}The latitude coordinate of the educational institution.{% enddocs %}
{% docs column__institution_longitude %}The longitude coordinate of the educational institution.{% enddocs %}
