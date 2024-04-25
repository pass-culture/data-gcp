{% macro ml_vars(var_name) -%}
    {%- 
        set ml_variables = {
            "diversification_features" : 
            var("diversification_features",
                [ "category"
                , "sub_category"
                , "format"
                , "venue_id"
                , "extra_category"
                ,])
        } 
     -%}

     {{ return(ml_variables[var_name]) }}
{%- endmacro %}