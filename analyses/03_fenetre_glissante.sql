-- Q3: La consommation régionale (GWh) du mois écoulé, chaque jour (fenêtre glissante)

SELECT
    DATE_TRUNC('day', date_heure) AS date_jour,
    libelle_region AS region,
    ROUND(SUM(SUM(consommation)) OVER (
        PARTITION BY libelle_region
        ORDER BY DATE_TRUNC('day', date_heure)
        RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
    ) / 1000) AS 'consommation_30j_glissants (GWh)'
FROM
    eco2mix.eco2mix_cleaned
WHERE
    date_heure >= '2024-02-15' AND date_heure < '2024-03-15'
GROUP BY
    date_jour,
    libelle_region
ORDER BY
    date_jour,
    libelle_region;
