-- Q7a: Construction du cube avec ROLLUP

WITH donnees_enrichies AS (
    SELECT
        STRFTIME(date_heure, '%Y-%m-%d') AS jour,                      
        STRFTIME(date_heure, '%Y-%m') AS mois,
        STRFTIME(date_heure, '%Y') AS annee, 
        CASE 
            WHEN libelle_region IN ('Bretagne', 'Normandie', 'Pays de la Loire', 'Centre-Val de Loire') THEN 'NO'
            WHEN libelle_region IN ('Hauts-de-France', 'Grand Est', 'Bourgogne-Franche-Comté') THEN 'NE'
            WHEN libelle_region IN ('Nouvelle-Aquitaine', 'Occitanie') THEN 'SO'
            WHEN libelle_region IN ('Auvergne-Rhône-Alpes', 'Provence-Alpes-Côte d''Azur') THEN 'SE'
            WHEN libelle_region = 'Île-de-France' THEN 'IdF'
            ELSE 'Autre'
        END AS zone,
        libelle_region,
        consommation
    FROM
        eco2mix.eco2mix_cleaned
)
SELECT
    annee,
    mois,
    jour,
    zone,
    libelle_region,
    ROUND(SUM(consommation) / 1000, 2) AS "Consommation (GWh)"
FROM
    donnees_enrichies
GROUP BY ROLLUP (annee, mois, jour, zone, libelle_region)
ORDER BY
    annee NULLS LAST,
    mois NULLS LAST,
    jour NULLS LAST,
    zone NULLS LAST,
    libelle_region NULLS LAST;
