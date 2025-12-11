{{ config(materialized='view') }}

SELECT 
    code_insee_region,
    libelle_region,
    nature,
    date_heure,
    consommation,
    COALESCE(thermique, 0) AS thermique,
    COALESCE(nucleaire, 0) AS nucleaire,
    COALESCE(solaire, 0) AS solaire,
    COALESCE(hydraulique, 0) AS hydraulique,
    COALESCE(pompage, 0) AS pompage,
    COALESCE(bioenergies, 0) AS bioenergies,
    COALESCE(eolien, 0) AS eolien,
    ech_physiques,

    -- TCO Thermique (% de la consommation)
    CASE
        WHEN tco_thermique IS NOT NULL THEN tco_thermique
        WHEN consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(COALESCE(thermique, 0) / consommation * 100)
    END AS tco_thermique,

    -- TCH Thermique (% de la capacité max de la région)
    CASE
        WHEN MAX(COALESCE(thermique, 0)) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(COALESCE(thermique, 0) / MAX(COALESCE(thermique, 0)) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_thermique,

    -- TCO Nucléaire (% de la consommation)
    CASE
        WHEN tco_nucleaire IS NOT NULL THEN tco_nucleaire
        WHEN consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(COALESCE(nucleaire, 0) / consommation * 100)
    END AS tco_nucleaire,

    -- TCH Nucléaire (% de la capacité max de la région)
    CASE
        WHEN MAX(COALESCE(nucleaire, 0)) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(COALESCE(nucleaire, 0) / MAX(COALESCE(nucleaire, 0)) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_nucleaire,

    -- TCO Eolien (% de la consommation)
    CASE
        WHEN tco_eolien IS NOT NULL THEN tco_eolien
        WHEN consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(COALESCE(eolien, 0) / consommation * 100)
    END AS tco_eolien,

    -- TCH Eolien (% de la capacité max de la région)
    CASE
        WHEN MAX(COALESCE(eolien, 0)) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(COALESCE(eolien, 0) / MAX(COALESCE(eolien, 0)) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_eolien,

    -- TCO Solaire (% de la consommation)
    CASE
        WHEN tco_solaire IS NOT NULL THEN tco_solaire
        WHEN consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(COALESCE(solaire, 0) / consommation * 100)
    END AS tco_solaire,

    -- TCH Solaire (% de la capacité max de la région)
    CASE
        WHEN MAX(COALESCE(solaire, 0)) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(COALESCE(solaire, 0) / MAX(COALESCE(solaire, 0)) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_solaire,

    -- TCO Hydraulique (% de la consommation)
    CASE
        WHEN tco_hydraulique IS NOT NULL THEN tco_hydraulique
        WHEN consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(COALESCE(hydraulique, 0) / consommation * 100)
    END AS tco_hydraulique,

    -- TCH Hydraulique (% de la capacité max de la région)
    CASE
        WHEN MAX(COALESCE(hydraulique, 0)) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(COALESCE(hydraulique, 0) / MAX(COALESCE(hydraulique, 0)) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_hydraulique,

    -- TCO Bioénergies (% de la consommation)
    CASE
        WHEN tco_bioenergies IS NOT NULL THEN tco_bioenergies
        WHEN consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(COALESCE(bioenergies, 0) / consommation * 100)
    END AS tco_bioenergies,

    -- TCH Bioénergies (% de la capacité max de la région)
    CASE
        WHEN MAX(COALESCE(bioenergies, 0)) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(COALESCE(bioenergies, 0) / MAX(COALESCE(bioenergies, 0)) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_bioenergies
FROM 
    {{ ref('stg_eco2mix') }}
ORDER BY 
    date_heure