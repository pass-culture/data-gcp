{% macro discovery_vars(var_name) -%}
    {%- 
        set discovery_features = {
        "discovery_features" :
        var("discovery_features",
        [ "offer_category_id"
        , "offer_subcategory_id"
        , "new_items"
        ,])
}
-%}
     {{ return(discovery_features[var_name]) }}
{%- endmacro %}