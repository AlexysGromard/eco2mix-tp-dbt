{{ config(materialized='table') }}

SELECT 
    DATE(date_heure) AS jour,
    code_insee_region,
    libelle_region,
    nature,
    SUM(consommation) AS consommation_totale,
    SUM(thermique) AS thermique_totale,
    SUM(nucleaire) AS nucleaire_totale,
    SUM(solaire) AS solaire_totale,
    SUM(hydraulique) AS hydraulique_totale,
    SUM(pompage) AS pompage_totale,
    SUM(bioenergies) AS bioenergies_totale,
    SUM(eolien) AS eolien_totale,
    SUM(ech_physiques) AS ech_physiques_totaux,

    -- Agrégations pour les TCO et TCH
    AVG(tco_thermique) AS tco_thermique_moyen,
    AVG(tch_thermique) AS tch_thermique_moyen,
    AVG(tco_nucleaire) AS tco_nucleaire_moyen,
    AVG(tch_nucleaire) AS tch_nucleaire_moyen,
    AVG(tco_eolien) AS tco_eolien_moyen,
    AVG(tch_eolien) AS tch_eolien_moyen,
    AVG(tco_solaire) AS tco_solaire_moyen,
    AVG(tch_solaire) AS tch_solaire_moyen,
    AVG(tco_hydraulique) AS tco_hydraulique_moyen,
    AVG(tch_hydraulique) AS tch_hydraulique_moyen,
    AVG(tco_bioenergies) AS tco_bioenergies_moyen,
    AVG(tch_bioenergies) AS tch_bioenergies_moyen
FROM 
    {{ ref('int_eco2mix_tco_tch_corrected') }}
GROUP BY 
    jour, code_insee_region, libelle_region, nature
ORDER BY 
    jour