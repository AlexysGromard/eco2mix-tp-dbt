-- Q1: Production (en GWh) et consommation (en GWh), ainsi que leurs versions min, max et moyenne instantanées (en Mw), par mois et par région.

SELECT
    date_trunc('month', date_heure) AS mois_date,
    strftime(date_trunc('month', date_heure), '%B') AS mois,
    libelle_region AS region,

    -- Production instantanée (MW)
    MIN(thermique + nucleaire + solaire + hydraulique + pompage + bioenergies + eolien) AS 'min production (MW)',
    ROUND(AVG(thermique + nucleaire + solaire + hydraulique + pompage + bioenergies + eolien)) AS 'moy production (MW)',
    MAX(thermique + nucleaire + solaire + hydraulique + pompage + bioenergies + eolien) AS 'max production (MW)',

    -- Production totale (GWh)
    SUM(thermique + nucleaire + solaire + hydraulique + pompage + bioenergies + eolien) / 1000 AS 'production (GWh)',

    -- Consommation instantanée (MW)
    MIN(consommation) AS 'min consommation (MW)',
    ROUND(AVG(consommation)) AS 'moy consommation (MW)',
    MAX(consommation) AS 'max consommation (MW)',

    -- Consommation totale (GWh)
    SUM(consommation) / 1000 AS 'consommation (GWh)'

FROM
    eco2mix.eco2mix_cleaned
WHERE
    EXTRACT(YEAR FROM date_heure) = 2020
GROUP BY
    mois_date,
    mois, 
    libelle_region
ORDER BY
    mois_date,
    libelle_region;
