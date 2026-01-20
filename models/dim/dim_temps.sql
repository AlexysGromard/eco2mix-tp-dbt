{{ config(materialized='table') }}

WITH dates_uniques AS (
    SELECT DISTINCT jour AS date
    FROM {{ ref('int_eco2mix_cleaned_per_day') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY date) AS id_temps,
    date,
    EXTRACT(DAY FROM date) AS jour,
    EXTRACT(MONTH FROM date) AS mois,
    EXTRACT(YEAR FROM date) AS annee,
    EXTRACT(QUARTER FROM date) AS trimestre,

    -- Saison
    CASE 
        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Hiver'
        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'Printemps'
        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Été'
        WHEN EXTRACT(MONTH FROM date) IN (9, 10, 11) THEN 'Automne'
    END AS saison,

    -- Nom du mois
    CASE EXTRACT(MONTH FROM date)
        WHEN 1 THEN 'Janvier'
        WHEN 2 THEN 'Février'
        WHEN 3 THEN 'Mars'
        WHEN 4 THEN 'Avril'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Juin'
        WHEN 7 THEN 'Juillet'
        WHEN 8 THEN 'Août'
        WHEN 9 THEN 'Septembre'
        WHEN 10 THEN 'Octobre'
        WHEN 11 THEN 'Novembre'
        WHEN 12 THEN 'Décembre'
    END AS nom_mois,

    -- Jour de la semaine
    EXTRACT(DAYOFWEEK FROM date) AS jour_semaine_num,
    DAYNAME(date) AS jour_semaine_nom,

    -- Indicateurs
    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS est_weekend

FROM dates_uniques
ORDER BY date
