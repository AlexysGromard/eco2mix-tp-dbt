# Eco2mix - Projet dbt

Projet d'analyse des données éco2mix avec dbt et DuckDB.

## Prérequis

- Python 3.11+
- dbt-duckdb
- DuckDB CLI

## Installation

```bash
# Créer et activer l'environnement virtuel Python
python -m venv .venv
source .venv/bin/activate

# Installer les dépendances Python
pip install -r requirements.txt
```

## Commandes dbt

```bash
# Exécuter les modèles
dbt run

# Lancer les tests
dbt test

# Générer la documentation
dbt docs generate
dbt docs serve
```

## DuckDB

```bash
# Lancer DuckDB en mode interactif
duckdb eco2mix.duckdb -ui
```

L'interface web DuckDB sera accessible sur http://localhost:8080

## Structure du projet

```
├── models/           # Modèles dbt
│   ├── staging/      # Modèles de staging
│   └── intermediate/ # Modèles intermédiaires
├── seeds/            # Données statiques
├── tests/            # Tests personnalisés
└── eco2mix.duckdb    # Base de données DuckDB
```
