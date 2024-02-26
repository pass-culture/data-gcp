{% macro ml_vars(var_name) -%}
    {%- 
        set ml_variables = {
            "diversification_features" : 
            var("diversification_features",
                [ "category",
                "sub_category",
                "format",
                "venue_id",
                "extra_category",
                ,])
            ,
            "diversification_features2" : 
            var("diversification_features",
                [ -- legacy features 
                "category"
                , "sub_category"
                , "format"
                , "venue_id"
                , "extra_category"
                -- new features
                , "micro_category_details"
                , "macro_category_details"
                , "category_lvl0"
                , "category_lvl1"
                , "category_lvl2"
                , "category_genre_lvl1"
                , "category_genre_lvl2"
                , "category_medium_lvl1"
                , "category_medium_lvl2"
                ,])

        } 
     -%}

     {{ return(ml_variables[var_name]) }}
{%- endmacro %}