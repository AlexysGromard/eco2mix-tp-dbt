{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('eco2mix_temps_reel', 'regional_tr') }}
),

nettoyage_et_enrichissement AS (
    SELECT 
        -- Identification
        date_heure,
        code_insee_region,
        libelle_region,
        nature,

        -- Statut de la donnée (par défaut temps_reel)
        'temps_reel' AS statut_donnee,

        -- Date d'intégration dans le système
        CURRENT_TIMESTAMP AS date_integration,

        -- Consommation
        COALESCE(consommation, 0) AS consommation,

        -- Production par filière
        COALESCE(thermique, 0) AS thermique,
        COALESCE(nucleaire, 0) AS nucleaire,
        COALESCE(eolien, 0) AS eolien,
        COALESCE(solaire, 0) AS solaire,
        COALESCE(hydraulique, 0) AS hydraulique,
        COALESCE(TRY_CAST(pompage AS BIGINT), 0) AS pompage,
        COALESCE(bioenergies, 0) AS bioenergies,

        -- Taux de couverture
        tch_thermique,
        tch_nucleaire,
        tch_eolien,
        tch_solaire,
        tch_hydraulique,
        tch_bioenergies,

        -- Échanges
        COALESCE(ech_physiques, 0) AS ech_physiques,

        -- TCO
        tco_thermique,
        tco_nucleaire,
        tco_eolien,
        tco_solaire,
        tco_hydraulique,
        tco_bioenergies

    FROM source
    WHERE date_heure IS NOT NULL
        AND code_insee_region IS NOT NULL
)

SELECT * FROM nettoyage_et_enrichissement
