{{ config(materialized='table') }}

WITH temperatures_avec_intervalles AS (
    SELECT DISTINCT
        ROUND(tmoy, 0) AS temperature_degre,
        MIN(tmin) AS temperature_min,
        MAX(tmax) AS temperature_max,
        AVG(tmoy) AS temperature_moyenne,

        -- Calcul de l'intervalle de température
        CASE 
            WHEN AVG(tmoy) < 0 THEN 'Glacial'
            WHEN AVG(tmoy) >= 0 AND AVG(tmoy) < 8 THEN 'Froid'
            WHEN AVG(tmoy) >= 8 AND AVG(tmoy) < 17 THEN 'Modéré'
            WHEN AVG(tmoy) >= 17 AND AVG(tmoy) <= 25 THEN 'Idéal'
            WHEN AVG(tmoy) > 25 AND AVG(tmoy) < 33 THEN 'Chaud'
            WHEN AVG(tmoy) >= 33 THEN 'Extrême'
        END AS intervalle_temperature

    FROM {{ ref('stg_temp_quot_reg') }}
    GROUP BY ROUND(tmoy, 0)
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY temperature_degre) AS id_temperature,
    temperature_degre,
    temperature_min,
    temperature_max,
    temperature_moyenne,
    intervalle_temperature,

FROM temperatures_avec_intervalles
ORDER BY temperature_degre
