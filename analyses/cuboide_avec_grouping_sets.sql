-- Cuboïde avec GROUPING SETS

SELECT
    -- Dimension temps
    dt.annee,
    dt.mois,
    dt.nom_mois,

    -- Dimension géographie
    dg.quart,

    -- Dimension température
    dtemp.intervalle_temperature,

    -- Mesures agrégées
    ROUND(SUM(f.consommation_gwh), 2) AS consommation_totale_gwh,
    ROUND(SUM(f.production_gwh), 2) AS production_totale_gwh,
    ROUND(AVG(f.taux_couverture), 2) AS taux_couverture_moyen,
    ROUND(SUM(f.production_renouvelable_gwh), 2) AS production_renouvelable_gwh,

FROM {{ ref('fact_energie_quotidienne') }} f

INNER JOIN {{ ref('dim_temps') }} dt 
    ON f.id_temps = dt.id_temps

INNER JOIN {{ ref('dim_geographie') }} dg 
    ON f.id_geographie = dg.id_geographie

LEFT JOIN {{ ref('dim_temperature') }} dtemp 
    ON f.id_temperature = dtemp.id_temperature

GROUP BY GROUPING SETS (
    -- Niveau le plus détaillé : par mois, quart et température
    (dt.annee, dt.mois, dt.nom_mois, dg.quart, dtemp.intervalle_temperature),

    -- Par mois et quart (toutes températures)
    (dt.annee, dt.mois, dt.nom_mois, dg.quart),

    -- Par mois et température (tous quarts)
    (dt.annee, dt.mois, dt.nom_mois, dtemp.intervalle_temperature),

    -- Par quart et température (tous mois)
    (dg.quart, dtemp.intervalle_temperature),

    -- Par mois seulement
    (dt.annee, dt.mois, dt.nom_mois),

    -- Par quart seulement
    (dg.quart),

    -- Par température seulement
    (dtemp.intervalle_temperature),

    -- Total général
    ()
)

ORDER BY
    dt.annee NULLS LAST,
    dt.mois NULLS LAST,
    dg.quart NULLS LAST,
    dtemp.intervalle_temperature NULLS LAST
