select objective_name, objective_type, region_name, department_code, objective
from `{{ bigquery_seed_dataset }}.departmental_objectives`
