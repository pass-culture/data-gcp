select 
    item_id,
    offer_name,
    offer_description,
    titelive_gtl_id as gtl_id,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4
from {project}.{ml_input_dataset}.item_metadata
where gtl_type = 'BOOK' and titelive_gtl_id is not null