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
                [ "category"
                , "topic_id"
                , "cluster_id"
                , "venue_type_label"
                ,])
            ,
            "diversification_features3" : 
            var("diversification_features3",
                [ "category"
                ,"simple_topic_id"
                , "simple_cluster_id"
                , "venue_type_label"
                ,])
            ,
            "diversification_features4" : 
            var("diversification_features4",
                [ "category"
                , "unconstrained_topic_id"
                , "unconstrained_cluster_id"
                , "venue_type_label"
                ,])
            ,
            "diversification_features5" : 
            var("diversification_features5",
                [ "category"
                , "unconstrained_topic_id"
                , "venue_type_label"
                ,])
            ,
            "diversification_features6" : 
            var("diversification_features6",
                [ "category"
                , "unconstrained_cluster_id"
                , "venue_type_label"
                ,])

        } 
     -%}

     {{ return(diversification_variables[var_name]) }}
{%- endmacro %}