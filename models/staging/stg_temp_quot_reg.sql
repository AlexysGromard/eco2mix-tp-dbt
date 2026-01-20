{{ config(materialized='view') }}

SELECT 
    * EXCLUDE (
        id
    ),
FROM {{ source('temperature_raw', 'quotidienne_regionale') }}
ORDER BY 
    date,
    region