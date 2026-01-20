-- Q6: Trouver toutes les périodes correspondant aux 3 plus longues séquences d'augmentation de la consommation instantanée

WITH RECURSIVE donnees_avec_marqueur AS (
    SELECT
        date_heure,
        libelle_region,
        consommation,
        LAG(consommation) OVER (PARTITION BY libelle_region ORDER BY date_heure) AS consommation_precedente,
        CASE 
            WHEN LAG(consommation) OVER (PARTITION BY libelle_region ORDER BY date_heure) IS NULL THEN 1
            WHEN consommation <= LAG(consommation) OVER (PARTITION BY libelle_region ORDER BY date_heure) THEN 1
            ELSE 0
        END AS debut_groupe
    FROM
        eco2mix.eco2mix_cleaned
),
groupes_identifies AS (
    SELECT
        date_heure,
        libelle_region,
        consommation,
        consommation_precedente,
        SUM(debut_groupe) OVER (PARTITION BY libelle_region ORDER BY date_heure) AS groupe_id
    FROM
        donnees_avec_marqueur
),
sequences_completes AS (
    SELECT
        MIN(date_heure) AS debut_sequence,
        MAX(date_heure) AS fin_sequence,
        libelle_region,
        COUNT(*) AS longueur,
        MAX(date_heure) - MIN(date_heure) AS duree,
        '[' || STRING_AGG(CAST(consommation AS VARCHAR), ', ' ORDER BY date_heure) || ']' AS sequence_valeurs
    FROM
        groupes_identifies
    WHERE
        consommation > consommation_precedente
    GROUP BY
        libelle_region,
        groupe_id
    HAVING
        COUNT(*) > 1  
)
SELECT
    debut_sequence AS "Date - Heure",
    duree AS "Durée (hh:mm:ss)",
    libelle_region AS "Région",
    sequence_valeurs AS "Séquence (MW*)",
    ROW_NUMBER() OVER (ORDER BY longueur DESC, duree DESC) AS "Rang"
FROM
    sequences_completes
ORDER BY
    longueur DESC,
    duree DESC
LIMIT 3;
