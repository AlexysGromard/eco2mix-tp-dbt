-- Q5: Jour du dépassement des énergies renouvelables, pour chaque année (2013 à 2024)

WITH production_annuelle_renouvelable AS (
    SELECT
        EXTRACT(YEAR FROM date_heure) AS annee,
        SUM(solaire + hydraulique + pompage + bioenergies + eolien) / 1000 AS production_renouvelable_annuelle_GWh
    FROM
        eco2mix.eco2mix_cleaned
    GROUP BY
        annee
),
consommation_cumulee AS (
    SELECT
        DATE_TRUNC('day', date_heure) AS jour,
        EXTRACT(YEAR FROM date_heure) AS annee,
        SUM(SUM(consommation)) OVER (
            PARTITION BY EXTRACT(YEAR FROM date_heure)
            ORDER BY DATE_TRUNC('day', date_heure)
        ) / 1000 AS consommation_cumulee_GWh
    FROM
        eco2mix.eco2mix_cleaned
    GROUP BY
        jour,
        annee
)
SELECT
    cc.annee,
    MIN(cc.jour) AS date_depassement,
    par.production_renouvelable_annuelle_GWh,
    MIN(cc.consommation_cumulee_GWh) AS consommation_cumulee_au_depassement_GWh
FROM
    consommation_cumulee cc
JOIN
    production_annuelle_renouvelable par ON cc.annee = par.annee
WHERE
    cc.consommation_cumulee_GWh >= par.production_renouvelable_annuelle_GWh
GROUP BY
    cc.annee,
    par.production_renouvelable_annuelle_GWh
ORDER BY
    cc.annee;
