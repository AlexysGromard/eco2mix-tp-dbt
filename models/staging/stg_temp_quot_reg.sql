{{ config(materialized='view') }}

SELECT 
    * EXCLUDE (
        id,
        code_insee_region,
    ),
    CAST(code_insee_region AS VARCHAR) AS code_insee_region,
FROM {{ source('temperature_raw', 'quotidienne_regionale') }}
ORDER BY 
    date,
    region