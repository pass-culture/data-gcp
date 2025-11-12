select
    question_id,
    question_text,
    cast(json_extract_array(choices) as array<string>) as choices_array
from {{ source('gcs_seeds', 'qpi_questions_v2') }}
