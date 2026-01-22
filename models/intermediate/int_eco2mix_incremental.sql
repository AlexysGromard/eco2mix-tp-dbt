{{ config(
    materialized='incremental',
    unique_key=['jour', 'code_insee_region', 'statut_donnee'],
    on_schema_change='append_new_columns'
) }}

/*
Modèle incrémental pour la table de fait avec gestion des statuts de données.
Supporte le cycle: temps_reel → consolidee → definitive

Logique de mise à jour:
- Les données avec un statut supérieur écrasent les données avec un statut inférieur
- Pour une même date/région, on garde le statut le plus élevé
*/

WITH nouvelles_donnees AS (
    SELECT 
        -- Extraction de la date depuis date_heure
        DATE_TRUNC('day', date_heure) AS jour,
        code_insee_region,
        libelle_region,
        statut_donnee,
        date_integration,
        
        -- Agrégation journalière des mesures (somme ou moyenne selon le cas)
        SUM(consommation) AS consommation_totale,
        SUM(thermique) AS thermique_totale,
        SUM(nucleaire) AS nucleaire_totale,
        SUM(solaire) AS solaire_totale,
        SUM(hydraulique) AS hydraulique_totale,
        SUM(bioenergies) AS bioenergies_totale,
        SUM(eolien) AS eolien_totale,
        SUM(pompage) AS pompage_totale,
        SUM(ech_physiques) AS ech_physiques_totaux,
        
        -- Taux de couverture moyens
        AVG(tco_thermique) AS tco_thermique_moyen,
        AVG(tco_nucleaire) AS tco_nucleaire_moyen,
        AVG(tco_eolien) AS tco_eolien_moyen,
        AVG(tco_solaire) AS tco_solaire_moyen,
        AVG(tco_hydraulique) AS tco_hydraulique_moyen,
        AVG(tco_bioenergies) AS tco_bioenergies_moyen,
        
        -- Métadonnées pour traçabilité
        MAX(date_integration) AS derniere_maj
        
    FROM {{ ref('stg_eco2mix_temps_reel') }}
    
    {% if is_incremental() %}
    -- En mode incrémental, on ne traite que les nouvelles données
    WHERE date_integration > (SELECT MAX(derniere_maj) FROM {{ this }})
    {% endif %}
    
    GROUP BY jour, code_insee_region, libelle_region, statut_donnee, date_integration
),

donnees_avec_priorite AS (
    SELECT 
        *,
        -- Ordre de priorité des statuts (plus élevé = plus fiable)
        CASE statut_donnee
            WHEN 'definitive' THEN 3
            WHEN 'consolidee' THEN 2
            WHEN 'temps_reel' THEN 1
            ELSE 0
        END AS priorite_statut
    FROM nouvelles_donnees
)

{% if is_incremental() %}
-- En mode incrémental, fusionner avec les données existantes
,fusion_donnees AS (
    SELECT * FROM donnees_avec_priorite
    
    UNION ALL
    
    SELECT 
        jour,
        code_insee_region,
        libelle_region,
        statut_donnee,
        date_integration,
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
        derniere_maj,
        CASE statut_donnee
            WHEN 'definitive' THEN 3
            WHEN 'consolidee' THEN 2
            WHEN 'temps_reel' THEN 1
            ELSE 0
        END AS priorite_statut
    FROM {{ this }}
)

-- Garder seulement la version avec le statut le plus élevé
,donnees_finales AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY jour, code_insee_region 
                ORDER BY priorite_statut DESC, derniere_maj DESC
            ) AS rn
        FROM fusion_donnees
    )
    WHERE rn = 1
)
{% endif %}

SELECT 
    jour,
    code_insee_region,
    libelle_region,
    statut_donnee,
    date_integration,
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
    derniere_maj
FROM {% if is_incremental() %}donnees_finales{% else %}donnees_avec_priorite{% endif %}
