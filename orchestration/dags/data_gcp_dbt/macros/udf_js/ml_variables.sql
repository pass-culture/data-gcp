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
            ,
            "diversification_features2" : 
            var("diversification_features2",
                [ "topic_id"
                , "cluster_id"
                , "venue_type_label"
                ,])

        } 
     -%}

     {{ return(ml_variables[var_name]) }}
{%- endmacro %}