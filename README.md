# 📊 Eco2mix - Analyse de données énergétiques

## 🎯 À propos

Projet d'analyse et de modélisation des données de production et consommation électrique en France métropolitaine, développé dans le cadre du TP **SQL avancé et entrepôts de données**.

Ce projet implémente un **Data Warehouse** complet avec :
- 🔧 **dbt-core** pour la transformation et la modélisation des données
- 🦆 **DuckDB** comme moteur analytique OLAP haute performance  
- ⭐ **Schéma en étoile** multi-dimensionnel (temps, géographie, température)
- 📈 **Requêtes SQL avancées** (window functions, CTE récursives, CUBE/ROLLUP)

> Malgré de nombreuses tentatives, nous n'avons pas réussi à faire fonctionner evidence.

### 📊 Données sources
- **éCO2mix régional consolidé** : production et consommation électrique par région (2013-2024)
- **Températures quotidiennes** : relevés météorologiques régionaux (2016-2024)
- **12+ ans d'historique** : millions de points de mesure consolidés

## Prérequis

- Python 3.11+
- dbt-core
- dbt-duckdb
- DuckDB CLI

## Installation

```bash
# Créer et activer l'environnement virtuel Python
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# ou .venv\Scripts\activate  # Windows

# Installer les dépendances Python
pip install -r requirements.txt
```

## Configuration

Le projet utilise DuckDB comme base de données. La configuration se fait dans le fichier `~/.dbt/profiles.yml` :

```yaml
eco2mix:
  outputs:
    dev:
      type: duckdb
      path: eco2mix.duckdb
      extensions:
        - parquet
  target: dev
```

## Commandes dbt

```bash
# Exécuter les modèles
dbt run

# Lancer les tests
dbt test

# Lancer les tests pour un modèle spécifique
dbt test --select stg_eco2mix
dbt test --select dim_temps
dbt test --select fact_energie_quotidienne

# Générer la documentation
dbt docs generate
dbt docs serve

# Compiler les modèles SQL en fichiers .sql
dbt compile
```

## Tests

Le projet contient des tests de qualité de données pour garantir l'intégrité :

### Tests implémentés
- **Tests d'unicité** : Clés primaires des dimensions (id_temps, id_geographie, id_temperature)
- **Tests de non-nullité** : Colonnes essentielles (dates, codes région, mesures)
- **Tests de relations** : Intégrité référentielle entre faits et dimensions
- **Tests de valeurs acceptées** : Validation des statuts de données (temps_reel, consolidee, definitive)

### Lancer les tests

```bash
# Tous les tests
dbt test

# Tests par couche
dbt test --select staging.*
dbt test --select dim.*
dbt test --select mart.*

# Tests avec détails en cas d'échec
dbt test --store-failures
```

## DuckDB

```bash
# Lancer DuckDB en mode interactif
duckdb eco2mix.duckdb

# Lancer DuckDB avec interface web
duckdb eco2mix.duckdb -ui
```

L'interface web DuckDB sera accessible sur http://localhost:8080

## Structure du projet

```
├── models/           # Modèles dbt
│   ├── staging/      # Couche de staging (sources brutes)
│   ├── intermediate/ # Couche intermédiaire (transformations)
│   ├── dim/          # Tables de dimensions
│   └── mart/         # Tables de faits (schéma en étoile)
├── analyses/         # Requêtes analytiques SQL avancées
├── seeds/            # Données statiques
├── tests/            # Tests personnalisés
├── macros/           # Macros Jinja réutilisables
└── eco2mix.duckdb    # Base de données DuckDB
```

## Analyses SQL avancées

Les requêtes SQL avancées répondant aux questions du sujet se trouvent dans le dossier [`analyses/`](analyses/) :

### Section 3 - Exploration

1. **[Groupement et agrégation simples](analyses/01_groupement_aggregation.sql)** : Production et consommation (GWh) avec min/max/moyenne instantanées (MW), par mois et par région

2. **[Pivot](analyses/02_pivot_consommation.sql)** : Consommation journalière détaillée par région (format pivot avec une colonne par région)

3. **[Fenêtre glissante](analyses/03_fenetre_glissante.sql)** : Consommation régionale sur 30 jours glissants avec window functions

4. **[Variation](analyses/04_variation_consommation.sql)** : Top 20 des plus grands écarts de consommation quotidienne d'un jour à l'autre

5. **[Quantité cumulée](analyses/05_quantite_cumulee.sql)** : Date de dépassement de la production renouvelable annuelle par la consommation

6. **[Calcul de point fixe](analyses/06_calcul_point_fixe.sql)** : Les 3 plus longues séquences d'augmentation de consommation instantanée (CTE récursive)

7. **Construction du cube** : Consommation agrégée par dimensions temporelles et géographiques
   - [7a - ROLLUP](analyses/07a_cuboide_rollup.sql)
   - [7b - GROUPING SETS](analyses/07b_cuboide_grouping_sets.sql)
   - [7c - CUBE](analyses/07c_cuboide_cube.sql)

### Section 4 - Entrepôt de données

Le cuboïde par mois, quart et intervalle de température se trouve dans :
- **[Cuboïde mois/quart/température](analyses/cuboide_avec_grouping_sets.sql)**

## Schéma en étoile

Le projet implémente un schéma en étoile multi-dimensionnel avec :

### Tables de dimensions
- **[dim_temps](models/dim/dim_temps.sql)** : Dimension temporelle (jour, mois, saison, année)
- **[dim_geographie](models/dim/dim_geographie.sql)** : Dimension géographique (région, zone, pays)
- **[dim_temperature](models/dim/dim_temperature.sql)** : Dimension température (intervalle de température)

### Table de faits
- **[fact_energie_quotidienne](models/mart/fact_energie_quotidienne.sql)** : Mesures quotidiennes de production et consommation par région

## Sources de données

- [éCO2mix régional consolidé et définitif](https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-cons-def/) (2013-2024)
- [Température quotidienne régionale](https://odre.opendatasoft.com/explore/dataset/temperature-quotidienne-regionale/) (2016-2024)
- [éCO2mix régional temps réel](https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-tr/) (pour mises à jour incrémentales)

## Mise à Jour Incrémentale de l'Entrepôt

### Principe

Mise à jour incrémentale avec gestion du cycle : **temps_reel** → **consolidee** → **definitive**

**Règle** : Pour une même date/région, la donnée avec le statut de priorité la plus élevée est conservée (definitive=3, consolidee=2, temps_reel=1).

### Architecture

```
eco2mix-regional-tr.parquet
    ↓
stg_eco2mix_temps_reel (view)
    ↓
int_eco2mix_incremental (incremental) ← Agrégation journalière + gestion statuts
    ↓
fact_energie_quotidienne_incremental (incremental) ← Fusion avec données définitives
```

### Utilisation

#### Première exécution
```bash
dbt run --select fact_energie_quotidienne_incremental --full-refresh
```

#### Mise à jour quotidienne
```bash
dbt run --select fact_energie_quotidienne_incremental
```

Ne traite que les nouvelles données (filtrage sur `date_integration`), gain ~30×.

#### Exemple de transition de statut
```
Jour J   : 2025-01-15, Île-de-France, temps_reel, 5000 GWh
Jour J+1 : 2025-01-15, Île-de-France, consolidee, 5100 GWh
Résultat : La version consolidée remplace la version temps_reel
```

### Tests

```sql
-- Vérifier l'unicité
SELECT date, code_insee_region, COUNT(*) 
FROM {{ ref('fact_energie_quotidienne_incremental') }}
GROUP BY date, code_insee_region
HAVING COUNT(*) > 1
```

```bash
dbt test --select fact_energie_quotidienne_incremental
```

### Monitoring

```sql
-- Distribution par statut
SELECT statut_donnee, COUNT(*), MIN(date), MAX(date)
FROM fact_energie_quotidienne_incremental
GROUP BY statut_donnee;
```

### Composants

- **sources.yml** : Déclaration de `eco2mix-regional-tr.parquet`
- **stg_eco2mix_temps_reel.sql** : Nettoyage + ajout `statut_donnee` et `date_integration`
- **int_eco2mix_incremental.sql** : Agrégation journalière avec gestion priorités
- **fact_energie_quotidienne_incremental.sql** : Table de fait finale
- **macros/incremental_helpers.sql** : Fonctions utilitaires

## Documentation complète

Le sujet complet du TP est disponible dans [sujet_eco2mix_dbt_part2.md](sujet_eco2mix_dbt_part2.md).
