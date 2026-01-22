{{ config(
    materialized='incremental',
    unique_key=['date', 'code_insee_region'],
    on_schema_change='append_new_columns'
) }}

/*
Table de fait incrémentale pour les données éCO2mix.
Fusionne les données définitives historiques avec les données temps réel/consolidées.
*/

WITH donnees_definitives AS (
    -- Données historiques définitives (ne changent plus)
    SELECT 
        jour,
        code_insee_region,
        libelle_region,
        consommation_totale,
        thermique_totale,
        nucleaire_totale,
        solaire_totale,
        hydraulique_totale,
        bioenergies_totale,
        eolien_totale,
        pompage_totale,
        ech_physiques_totaux,
        tco_thermique_moyen,
        tco_nucleaire_moyen,
        tco_eolien_moyen,
        tco_solaire_moyen,
        tco_hydraulique_moyen,
        tco_bioenergies_moyen,
        'definitive' AS statut_donnee,
        CURRENT_TIMESTAMP AS derniere_maj
    FROM {{ ref('int_eco2mix_cleaned_per_day') }}
),

donnees_temps_reel AS (
    -- Données temps réel/consolidées (peuvent être mises à jour)
    SELECT 
        jour,
        code_insee_region,
        libelle_region,
        consommation_totale,
        thermique_totale,
        nucleaire_totale,
        solaire_totale,
        hydraulique_totale,
        bioenergies_totale,
        eolien_totale,
        pompage_totale,
        ech_physiques_totaux,
        tco_thermique_moyen,
        tco_nucleaire_moyen,
        tco_eolien_moyen,
        tco_solaire_moyen,
        tco_hydraulique_moyen,
        tco_bioenergies_moyen,
        statut_donnee,
        derniere_maj
    FROM {{ ref('int_eco2mix_incremental') }}
    
    {% if is_incremental() %}
    WHERE derniere_maj > (SELECT MAX(derniere_maj) FROM {{ this }})
    {% endif %}
),

donnees_unifiees AS (
    -- Fusion des deux sources
    SELECT * FROM donnees_definitives

    UNION ALL

    SELECT * FROM donnees_temps_reel
),

donnees_avec_priorite AS (
    SELECT 
        *,
        CASE statut_donnee
            WHEN 'definitive' THEN 3
            WHEN 'consolidee' THEN 2
            WHEN 'temps_reel' THEN 1
            ELSE 0
        END AS priorite_statut
    FROM donnees_unifiees
),

-- Sélection de la meilleure version pour chaque jour/région
meilleure_version AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY jour, code_insee_region 
                ORDER BY priorite_statut DESC, derniere_maj DESC
            ) AS rn
        FROM donnees_avec_priorite
    )
    WHERE rn = 1
),

energie_par_jour AS (
    SELECT 
        jour AS date,
        code_insee_region,
        libelle_region AS region,
        statut_donnee,

        -- Conversion MW en GWh (agrégation sur 24h)
        ROUND(consommation_totale / 1000.0, 2) AS consommation_gwh,
        ROUND((thermique_totale + nucleaire_totale + solaire_totale + 
               hydraulique_totale + bioenergies_totale + eolien_totale) / 1000.0, 2) AS production_gwh,

        -- Taux de couverture global
        CASE 
            WHEN consommation_totale = 0 THEN NULL
            ELSE ROUND(((thermique_totale + nucleaire_totale + solaire_totale + 
                        hydraulique_totale + bioenergies_totale + eolien_totale) / consommation_totale) * 100, 2)
        END AS taux_couverture,

        -- Production par filière en GWh
        ROUND(thermique_totale / 1000.0, 2) AS production_thermique_gwh,
        ROUND(nucleaire_totale / 1000.0, 2) AS production_nucleaire_gwh,
        ROUND(solaire_totale / 1000.0, 2) AS production_solaire_gwh,
        ROUND(hydraulique_totale / 1000.0, 2) AS production_hydraulique_gwh,
        ROUND(bioenergies_totale / 1000.0, 2) AS production_bioenergies_gwh,
        ROUND(eolien_totale / 1000.0, 2) AS production_eolien_gwh,
        ROUND(pompage_totale / 1000.0, 2) AS pompage_gwh,
        ROUND(ech_physiques_totaux / 1000.0, 2) AS echanges_physiques_gwh,

        -- Production renouvelable totale
        ROUND((solaire_totale + hydraulique_totale + bioenergies_totale + eolien_totale) / 1000.0, 2) AS production_renouvelable_gwh,

        -- Taux de couverture moyens par filière
        tco_thermique_moyen,
        tco_nucleaire_moyen,
        tco_eolien_moyen,
        tco_solaire_moyen,
        tco_hydraulique_moyen,
        tco_bioenergies_moyen,

        -- Métadonnées
        derniere_maj

    FROM meilleure_version
)

SELECT * 
FROM energie_par_jour
ORDER BY date, region
