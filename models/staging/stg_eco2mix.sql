{{ config(materialized='view') }}

SELECT 
    * EXCLUDE (
        column_30, 
        date, 
        heure, 
        stockage_batterie, 
        destockage_batterie, 
        eolien_terrestre, 
        eolien_offshore,
        eolien
    ), 
CASE 
    WHEN eolien = 'ND' THEN NULL
    WHEN eolien = '-' THEN NULL
    ELSE CAST(eolien AS INT)
END AS eolien
FROM {{ source('eco2mix_raw', 'regional_cons_def') }}
WHERE consommation IS NOT NULL