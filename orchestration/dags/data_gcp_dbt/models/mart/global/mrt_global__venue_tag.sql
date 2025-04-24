select
    vc.venue_id,
    v.offerer_id,
    v.partner_id,
    v.offerer_rank_desc,
    vc.criterion_id as venue_tag_id,
    ccm.criterion_category_id as venue_tag_category_id,
    cc.criterion_category_label as venue_tag_category_label,
    c.name as venue_tag_name
from {{ source("raw", "applicative_database_venue_criterion") }} as vc
left join {{ ref("mrt_global__venue") }} as v on vc.venue_id = v.venue_id
inner join
    {{ source("raw", "applicative_database_criterion_category_mapping") }} as ccm
    on ccm.criterion_id = vc.criterion_id
inner join
    {{ source("raw", "applicative_database_criterion_category") }} as cc
    on cc.criterion_category_id = ccm.criterion_category_id
inner join
    {{ source("raw", "applicative_database_criterion") }} as c
    on ccm.criterion_id = c.id
