{{ config(materialized='view') }}

SELECT 
    code_insee_region,
    libelle_region,
    nature,
    date_heure,
    consommation,
    thermique,
    nucleaire,
    solaire,
    hydraulique,
    pompage,
    bioenergies,
    eolien,
    ech_physiques,

    -- TCO Thermique (% de la consommation)
    CASE
        WHEN tco_thermique IS NOT NULL THEN tco_thermique
        WHEN thermique IS NULL OR consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(thermique / consommation * 100)
    END AS tco_thermique,

    -- TCH Thermique (% de la capacité max de la région)
    CASE
        WHEN thermique IS NULL OR MAX(thermique) OVER (PARTITION BY code_insee_region) IS NULL 
             OR MAX(thermique) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(thermique / MAX(thermique) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_thermique,

    -- TCO Nucléaire (% de la consommation)
    CASE
        WHEN tco_nucleaire IS NOT NULL THEN tco_nucleaire
        WHEN nucleaire IS NULL OR consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(nucleaire / consommation * 100)
    END AS tco_nucleaire,

    -- TCH Nucléaire (% de la capacité max de la région)
    CASE
        WHEN nucleaire IS NULL OR MAX(nucleaire) OVER (PARTITION BY code_insee_region) IS NULL 
             OR MAX(nucleaire) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(nucleaire / MAX(nucleaire) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_nucleaire,

    -- TCO Eolien (% de la consommation)
    CASE
        WHEN tco_eolien IS NOT NULL THEN tco_eolien
        WHEN eolien IS NULL OR consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(eolien / consommation * 100)
    END AS tco_eolien,

    -- TCH Eolien (% de la capacité max de la région)
    CASE
        WHEN eolien IS NULL OR MAX(eolien) OVER (PARTITION BY code_insee_region) IS NULL 
             OR MAX(eolien) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(eolien / MAX(eolien) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_eolien,

    -- TCO Solaire (% de la consommation)
    CASE
        WHEN tco_solaire IS NOT NULL THEN tco_solaire
        WHEN solaire IS NULL OR consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(solaire / consommation * 100)
    END AS tco_solaire,

    -- TCH Solaire (% de la capacité max de la région)
    CASE
        WHEN solaire IS NULL OR MAX(solaire) OVER (PARTITION BY code_insee_region) IS NULL 
             OR MAX(solaire) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(solaire / MAX(solaire) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_solaire,

    -- TCO Hydraulique (% de la consommation)
    CASE
        WHEN tco_hydraulique IS NOT NULL THEN tco_hydraulique
        WHEN hydraulique IS NULL OR consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(hydraulique / consommation * 100)
    END AS tco_hydraulique,

    -- TCH Hydraulique (% de la capacité max de la région)
    CASE
        WHEN hydraulique IS NULL OR MAX(hydraulique) OVER (PARTITION BY code_insee_region) IS NULL 
             OR MAX(hydraulique) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(hydraulique / MAX(hydraulique) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_hydraulique,

    -- TCO Bioénergies (% de la consommation)
    CASE
        WHEN tco_bioenergies IS NOT NULL THEN tco_bioenergies
        WHEN bioenergies IS NULL OR consommation IS NULL OR consommation = 0 THEN 0
        ELSE round(bioenergies / consommation * 100)
    END AS tco_bioenergies,

    -- TCH Bioénergies (% de la capacité max de la région)
    CASE
        WHEN bioenergies IS NULL OR MAX(bioenergies) OVER (PARTITION BY code_insee_region) IS NULL 
             OR MAX(bioenergies) OVER (PARTITION BY code_insee_region) = 0 THEN 0
        ELSE round(bioenergies / MAX(bioenergies) OVER (PARTITION BY code_insee_region) * 100)
    END AS tch_bioenergies
FROM 
    {{ ref('stg_eco2mix') }}
ORDER BY 
    date_heure