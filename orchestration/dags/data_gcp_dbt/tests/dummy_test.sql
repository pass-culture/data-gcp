{{ config(severity = 'error',error_if= ">=10") }}
select * from {{ref('siren_data')}}