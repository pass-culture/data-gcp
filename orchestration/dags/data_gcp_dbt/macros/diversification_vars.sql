{% macro diversification_vars(var_name) -%}
    {%- 
        set diversification_variables = {
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

     {{ return(diversification_variables[var_name]) }}
{%- endmacro %}