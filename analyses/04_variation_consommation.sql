-- Q4: Les 20 plus grands écarts de consommation quotidienne (GWh), d'un jour à l'autre

SELECT 
    jour,
    libelle_region,
    consommation_totale - LAG(consommation_totale) OVER (
        PARTITION BY libelle_region 
        ORDER BY jour
    ) AS ecart_vs_jour_precedent
FROM 
    eco2mix.eco2mix_cleaned_per_day
ORDER BY 
    ABS(consommation_totale - LAG(consommation_totale) OVER (
        PARTITION BY libelle_region 
        ORDER BY jour
    )) DESC
LIMIT 20;
