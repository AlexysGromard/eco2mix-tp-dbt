{{ config(materialized='table') }}

WITH energie_par_jour AS (
    SELECT 
        jour,
        code_insee_region,
        libelle_region,

        -- Conversion MW en GWh (agrégation sur 24h)
        ROUND(consommation_totale / 1000.0, 2) AS consommation_gwh,
        ROUND((thermique_totale + nucleaire_totale + solaire_totale + 
               hydraulique_totale + bioenergies_totale + eolien_totale) / 1000.0, 2) AS production_gwh,

        -- Production par filière en GWh
        ROUND(thermique_totale / 1000.0, 2) AS production_thermique_gwh,
        ROUND(nucleaire_totale / 1000.0, 2) AS production_nucleaire_gwh,
        ROUND(solaire_totale / 1000.0, 2) AS production_solaire_gwh,
        ROUND(hydraulique_totale / 1000.0, 2) AS production_hydraulique_gwh,
        ROUND(bioenergies_totale / 1000.0, 2) AS production_bioenergies_gwh,
        ROUND(eolien_totale / 1000.0, 2) AS production_eolien_gwh,
        ROUND(pompage_totale / 1000.0, 2) AS pompage_gwh,
        ROUND(ech_physiques_totaux / 1000.0, 2) AS echanges_physiques_gwh,

        -- Taux de couverture moyens
        tco_thermique_moyen,
        tco_nucleaire_moyen,
        tco_eolien_moyen,
        tco_solaire_moyen,
        tco_hydraulique_moyen,
        tco_bioenergies_moyen

    FROM {{ ref('int_eco2mix_cleaned_per_day') }}
),

temperature_par_jour AS (
    SELECT 
        date,
        code_insee_region,
        ROUND(tmoy, 0) AS temperature_degre,
        tmin,
        tmax,
        tmoy
    FROM {{ ref('stg_temp_quot_reg') }}
),

fait_avec_dimensions AS (
    SELECT 
        -- Clés des dimensions
        dt.id_temps,
        dg.id_geographie,
        dtemp.id_temperature,

        -- Métadonnées
        e.jour AS date,
        e.libelle_region AS region,

        -- Mesures de consommation et production
        e.consommation_gwh,
        e.production_gwh,

        -- Taux de couverture global
        CASE 
            WHEN e.consommation_gwh = 0 THEN NULL
            ELSE ROUND((e.production_gwh / e.consommation_gwh) * 100, 2)
        END AS taux_couverture,

        -- Production par filière
        e.production_thermique_gwh,
        e.production_nucleaire_gwh,
        e.production_solaire_gwh,
        e.production_hydraulique_gwh,
        e.production_bioenergies_gwh,
        e.production_eolien_gwh,
        e.pompage_gwh,
        e.echanges_physiques_gwh,

        -- Production renouvelable totale
        ROUND(e.production_solaire_gwh + e.production_hydraulique_gwh + 
              e.production_bioenergies_gwh + e.production_eolien_gwh, 2) AS production_renouvelable_gwh,

        -- Taux de couverture par filière
        e.tco_thermique_moyen,
        e.tco_nucleaire_moyen,
        e.tco_eolien_moyen,
        e.tco_solaire_moyen,
        e.tco_hydraulique_moyen,
        e.tco_bioenergies_moyen,

        -- Températures
        t.temperature_degre,
        t.tmin AS temperature_min,
        t.tmax AS temperature_max,
        t.tmoy AS temperature_moyenne

    FROM energie_par_jour e

    -- Jointures avec les dimensions
    INNER JOIN {{ ref('dim_temps') }} dt 
        ON e.jour = dt.date

    INNER JOIN {{ ref('dim_geographie') }} dg 
        ON e.code_insee_region = dg.code_insee_region

    LEFT JOIN temperature_par_jour t
        ON e.jour = t.date
        AND e.code_insee_region = t.code_insee_region

    LEFT JOIN {{ ref('dim_temperature') }} dtemp
        ON ROUND(t.tmoy, 0) = dtemp.temperature_degre
)

SELECT * 
FROM fait_avec_dimensions
ORDER BY date, region
