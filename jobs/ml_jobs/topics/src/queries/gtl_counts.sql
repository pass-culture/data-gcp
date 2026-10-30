select
    titelive_gtl_id,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4,
    count(*) as n_items
from `{project}.{ml_input_dataset}.item_metadata`
where gtl_type = 'BOOK' and titelive_gtl_id is not null
group by
    titelive_gtl_id,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4
