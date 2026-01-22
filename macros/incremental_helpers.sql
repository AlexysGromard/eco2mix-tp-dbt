{% macro get_statut_priorite(statut) %}
    CASE {{ statut }}
        WHEN 'definitive' THEN 3
        WHEN 'consolidee' THEN 2
        WHEN 'temps_reel' THEN 1
        ELSE 0
    END
{% endmacro %}

{% macro fusion_donnees_par_statut(cte_1, cte_2, partition_cols, order_cols=['priorite_statut', 'derniere_maj']) %}
/*
Macro pour fusionner deux CTEs de données avec gestion des priorités de statut.
Garde seulement la version avec le statut le plus élevé pour chaque partition.

Args:
    cte_1: Nom de la première CTE
    cte_2: Nom de la deuxième CTE
    partition_cols: Liste des colonnes de partition (ex: ['jour', 'code_insee_region'])
    order_cols: Liste des colonnes de tri pour la sélection (par défaut: priorité puis date)
*/

WITH fusion AS (
    SELECT * FROM {{ cte_1 }}
    UNION ALL
    SELECT * FROM {{ cte_2 }}
),

avec_rang AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY {{ partition_cols | join(', ') }}
            ORDER BY {{ order_cols | join(' DESC, ') }} DESC
        ) AS rn
    FROM fusion
)

SELECT * EXCEPT(rn)
FROM avec_rang
WHERE rn = 1

{% endmacro %}
