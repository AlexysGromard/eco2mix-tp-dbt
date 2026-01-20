{{ config(materialized='table') }}

WITH regions_uniques AS (
    SELECT DISTINCT 
        code_insee_region,
        libelle_region
    FROM {{ ref('int_eco2mix_cleaned_per_day') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY code_insee_region) AS id_geographie,
    'France' AS pays,
    -- Attribution des quarts géographiques
    CASE 
        WHEN libelle_region = 'Île-de-France' THEN 'IdF'
        WHEN libelle_region IN ('Hauts-de-France', 'Grand Est', 'Bourgogne-Franche-Comté') THEN 'NE'
        WHEN libelle_region IN ('Bretagne', 'Normandie', 'Pays de la Loire', 'Centre-Val de Loire') THEN 'NO'
        WHEN libelle_region IN ('Nouvelle-Aquitaine', 'Occitanie') THEN 'SO'
        WHEN libelle_region IN ('Auvergne-Rhône-Alpes', 'Provence-Alpes-Côte d''Azur') THEN 'SE'
        ELSE 'Autre'
    END AS quart,
    code_insee_region,
    libelle_region

FROM regions_uniques
ORDER BY code_insee_region
