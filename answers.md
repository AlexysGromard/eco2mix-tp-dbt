# Réponses des reuquêtes SQL
Le fichier `answers.md` contient les réponses aux différentes requêtes SQL. Étant donné que evidence n'a pas fonctionné correctement sur nos environnements, nous avons exécuté les requêtes SQL directement dans duckdb pour obtenir les résultats.

1. __Groupement et agrégation simples__ : Production (en GWh) et consommation (en GWh), ainsi que leurs versions min, max et moyenne instantanées (en Mw), par mois et par région.

```sql
-- Q1
SELECT
    date_trunc('month', date_heure) AS mois_date,
    strftime(date_trunc('month', date_heure), '%B') AS mois,
    libelle_region AS region,

    -- Production instantanée (MW)
    MIN(thermique + nucleaire + solaire + hydraulique + pompage + bioenergies + eolien) AS 'min production (MW)',
    ROUND(AVG(thermique + nucleaire + solaire + hydraulique + pompage + bioenergies + eolien)) AS 'moy production (MW)',
    MAX(thermique + nucleaire + solaire + hydraulique + pompage + bioenergies + eolien) AS 'max production (MW)',

    -- Production totale (GWh)
    SUM(thermique + nucleaire + solaire + hydraulique + pompage + bioenergies + eolien) / 1000 AS 'production (GWh)',

    -- Consommation instantanée (MW)
    MIN(consommation) AS 'min consommation (MW)',
    ROUND(AVG(consommation)) AS 'moy consommation (MW)',
    MAX(consommation) AS 'max consommation (MW)',

    -- Consommation totale (GWh)
    SUM(consommation) / 1000 AS 'consommation (GWh)'

FROM
    eco2mix.eco2mix_cleaned
WHERE
    EXTRACT(YEAR FROM date_heure) = 2020
GROUP BY
    mois_date,
    mois, 
    libelle_region
ORDER BY
    mois_date,
    libelle_region;
```

2. __Pivot__ (construction `CASE WHEN THEN END`) : consommation (GWh) journalière, détaillée par région. Par construction, chaque date devient une clé de la relation-résultat. Autrement dit, il y a une ligne de données de consommation pour chaque date et une colonne par région.

```sql
-- Q2
SELECT 
    CAST(date_heure AS DATE) as jour,

    SUM(CASE WHEN libelle_region = 'Auvergne-Rhône-Alpes' THEN consommation ELSE 0 END) as Auvergne_Rhône_Alpes,
    SUM(CASE WHEN libelle_region = 'Bourgogne-Franche-Comté' THEN consommation ELSE 0 END) as Bourgogne_Franche_Comté,
    SUM(CASE WHEN libelle_region = 'Bretagne' THEN consommation ELSE 0 END) as bretagne,
    SUM(CASE WHEN libelle_region = 'Centre-Val de Loire' THEN consommation ELSE 0 END) as Centre_Val_de_Loire,
    SUM(CASE WHEN libelle_region = 'Grand Est' THEN consommation ELSE 0 END) as grand_est,
    SUM(CASE WHEN libelle_region = 'Hauts-de-France' THEN consommation ELSE 0 END) as Hauts_de_France,
    SUM(CASE WHEN libelle_region = 'Île-de-France' THEN consommation ELSE 0 END) as ile_de_France,
    SUM(CASE WHEN libelle_region = 'Normandie' THEN consommation ELSE 0 END) as normandie,
    SUM(CASE WHEN libelle_region = 'Nouvelle-Aquitaine' THEN consommation ELSE 0 END) as Nouvelle_Aquitaine,
    SUM(CASE WHEN libelle_region = 'Occitanie' THEN consommation ELSE 0 END) as Occitanie,
    SUM(CASE WHEN libelle_region = 'Pays de la Loire' THEN consommation ELSE 0 END) as pdl,
    SUM(CASE WHEN libelle_region = 'Provence-Alpes-Côte d''Azur' THEN consommation ELSE 0 END) as Provence_Alpes_Cote_d_Azur

FROM eco2mix_cleaned
GROUP BY 1
ORDER BY 1
```

3. __Fenêtre glissante__ (_window function_ avec `RANGE`) : la consommation régionale (GWh) du mois écoulé, chaque jour. Par exemple, pour le 15 mars 2024, la consommation cumulée du 15 février au 15 mars 2024. Le résultat doit comporter une ligne par jour de la période considérée (2013-2024) et par région.
```sql
-- Q3
SELECT
    DATE_TRUNC('day', date_heure) AS date_jour,
    libelle_region AS region,
    ROUND(SUM(SUM(consommation)) OVER (
        PARTITION BY libelle_region
        ORDER BY DATE_TRUNC('day', date_heure)
        RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
    ) / 1000) AS 'consommation_30j_glissants (GWh)'
FROM
    eco2mix.eco2mix_cleaned
WHERE
    date_heure >= '2024-02-15' AND date_heure < '2024-03-15'
GROUP BY
    date_jour,
    libelle_region
ORDER BY
    date_jour,
    libelle_region;
```

4. __Variation__ (_window function_, avec _CTE_ pour décomposer le calcul) : les 20 plus grands écarts de consommation quotidienne (GWh), d'un jour à l'autre, toutes régions confondues.
```sql
-- Q4 : Écart de consommation entre un jour et le jour précédent PAR RÉGION
SELECT 
    jour,
    libelle_region,
    -- consommation_totale AS consommation_jour,
    -- LAG(consommation_totale) OVER (
    --    PARTITION BY libelle_region 
    --    ORDER BY jour
    --) AS consommation_jour_precedent,
    consommation_totale - LAG(consommation_totale) OVER (
        PARTITION BY libelle_region 
        ORDER BY jour
    ) AS ecart_vs_jour_precedent
FROM 
    eco2mix.eco2mix_cleaned_per_day
ORDER BY 
    ABS(consommation_totale - LAG(consommation_totale) OVER (
        PARTITION BY libelle_region 
        ORDER BY jour
    )) DESC
LIMIT 20;
```

5. __Quantité cumulée__ (_window functions_ + _CTE_) : jour du dépassement des énergies renouvelables, pour chaque année (de 2013 à 2024). En d'autres termes, à quel moment de l'année (une date) la consommation atteint - dépasse - la production annuelle totale des filières renouvelables ?

```sql
-- Q5
WITH production_annuelle_renouvelable AS (
    SELECT
        EXTRACT(YEAR FROM date_heure) AS annee,
        SUM(solaire + hydraulique + pompage + bioenergies + eolien) / 1000 AS production_renouvelable_annuelle_GWh
    FROM
        eco2mix.eco2mix_cleaned
    GROUP BY
        annee
),
consommation_cumulee AS (
    SELECT
        DATE_TRUNC('day', date_heure) AS jour,
        EXTRACT(YEAR FROM date_heure) AS annee,
        SUM(SUM(consommation)) OVER (
            PARTITION BY EXTRACT(YEAR FROM date_heure)
            ORDER BY DATE_TRUNC('day', date_heure)
        ) / 1000 AS consommation_cumulee_GWh
    FROM
        eco2mix.eco2mix_cleaned
    GROUP BY
        jour,
        annee
)
SELECT
    cc.annee,
    MIN(cc.jour) AS date_depassement,
    par.production_renouvelable_annuelle_GWh,
    MIN(cc.consommation_cumulee_GWh) AS consommation_cumulee_au_depassement_GWh
FROM
    consommation_cumulee cc
JOIN
    production_annuelle_renouvelable par ON cc.annee = par.annee
WHERE
    cc.consommation_cumulee_GWh >= par.production_renouvelable_annuelle_GWh
GROUP BY
    cc.annee,
    par.production_renouvelable_annuelle_GWh
ORDER BY
    cc.annee;
```

6. __Calcul de point fixe__ (_CTE récursive_) : trouver toutes les périodes correspondant aux 3 plus longues séquences d'augmentation de la consommation instantanée.

```sql
-- Q6
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
```

7. __Construction du cube__ (`GROUP BY CUBE|GROUPING SETS|ROLLUP`) : donner toutes les valeurs de consommation (en GWh) agrégés par jour, par mois, par année et sur toute la période, ainsi que par région, par zone (NO, NE, SO, SE et IdF) et sur l'ensemble du territoire métropolitain.

// TODO : A VÉRIFIER : LA REQUÊTE CI-DESSOUS NE SEMBLE PAS RESPECTER PLEINEMENT LE SUJET

```sql
-- Q7
WITH donnees_enrichies AS (
    SELECT
        EXTRACT(DAY FROM date_heure) AS jour,
        EXTRACT(MONTH FROM date_heure) AS mois,
        EXTRACT(YEAR FROM date_heure) AS annee,
        libelle_region,
        CASE 
            WHEN libelle_region IN ('Bretagne', 'Normandie', 'Pays de la Loire', 'Centre-Val de Loire') THEN 'NO'
            WHEN libelle_region IN ('Hauts-de-France', 'Grand Est', 'Bourgogne-Franche-Comté') THEN 'NE'
            WHEN libelle_region IN ('Nouvelle-Aquitaine', 'Occitanie') THEN 'SO'
            WHEN libelle_region IN ('Auvergne-Rhône-Alpes', 'Provence-Alpes-Côte d''Azur') THEN 'SE'
            WHEN libelle_region = 'Île-de-France' THEN 'IdF'
            ELSE 'Autre'
        END AS zone,
        consommation
    FROM
        eco2mix.eco2mix_cleaned
)
SELECT
    jour,
    mois,
    annee,
    libelle_region,
    zone,
    ROUND(SUM(consommation) / 1000, 2) AS "Consommation (GWh)",
    -- Indicateurs de niveau d'agrégation
    GROUPING(jour) AS grouping_jour,
    GROUPING(mois) AS grouping_mois,
    GROUPING(annee) AS grouping_annee,
    GROUPING(libelle_region) AS grouping_region,
    GROUPING(zone) AS grouping_zone
FROM
    donnees_enrichies
GROUP BY CUBE (
    (jour, mois, annee),
    (libelle_region, zone)
)
ORDER BY
    annee NULLS LAST,
    mois NULLS LAST,
    jour NULLS LAST,
    zone NULLS LAST,
    libelle_region NULLS LAST;
```