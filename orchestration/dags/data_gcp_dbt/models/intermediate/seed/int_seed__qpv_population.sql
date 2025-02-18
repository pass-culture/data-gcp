{{ config(**custom_table_config()) }}

select
    qpv.codeqpv as qpv_code,
    qpv.libqpv as qpv_name,
    qpv.codereg as qpv_region_code,
    qpv.codeuu2020 as qpv_urban_unit_2020_code,
    qpv.popmuniqpv as qpv_municipal_population_2020,
    (popMuniQPV * partPop15_24) / 100 as estimated_15_24_cohort,
    split(qpv.listedepcom, ',') as qpv_municipality_array,
    split(qpv.listelibdepcom, ',') as qpv_municipality_name_array,
    split(qpv.listeepci2022, ',') as qpv_epci_2022_array,
    split(qpv.listedep, ',') as qpv_department_array,
from {{ source("seed", "2024_insee_qpv_population") }} as qpv
