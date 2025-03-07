with
    home as (
        select
            date_imported,
            id as home_id,
            title as home_name,
            replace(modules, '\"', "") as module_id
        from
            {{ source("raw", "contentful_entry") }},
            unnest(json_extract_array(modules, '$')) as modules
        where content_type = "homepageNatif"
    ),

    home_and_modules as (
        select
            date(home.date_imported) as date,
            home_id,
            home_name,
            title as module_name,
            module_id,
            content_type
        from home
        left join
            {{ source("raw", "contentful_entry") }} module
            on home.module_id = module.id
            and home.date_imported = module.date_imported
    )

select date, home_id, home_name, module_name, module_id, content_type
from home_and_modules
qualify row_number() over (partition by date, home_id, module_id) = 1
