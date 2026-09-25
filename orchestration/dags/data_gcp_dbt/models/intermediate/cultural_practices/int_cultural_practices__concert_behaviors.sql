select
    ident18 as respondent_id,
    g3001 as alone,
    g3002 as with_partner,
    g3003 as with_children,
    g3004 as with_grandchildren,
    g3005 as with_relatives,
    g3006 as with_friends,
    g3007 as with_organized_group,
    g3008 as no_general_rule,
    case
        g31
        when 1
        then 'Oui, beaucoup'
        when 2
        then 'Oui, un peu'
        when 3
        then 'Non, pas tellement'
        when 4
        then 'Non, pas du tout'
        when 5
        then 'NSP'
    end as would_miss_concert
from {{ ref("int_seed__deps_cultural_practices_2018") }}
