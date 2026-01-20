{{ config(materialized='view') }}

SELECT 
    * EXCLUDE (
        id,
        code_insee_region,
        date
    ),
    CAST(code_insee_region AS VARCHAR) AS code_insee_region,
    CAST(date AS TIMESTAMPTZ) AS date
FROM {{ source('temperature_raw', 'quotidienne_regionale') }}
ORDER BY 
    date,
    region