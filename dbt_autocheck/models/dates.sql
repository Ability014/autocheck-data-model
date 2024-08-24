{{
    config(
        materialized = 'view'
    )
}}

{{ dbt_date.get_date_dimension('2018-01-01', '2025-12-31') }}