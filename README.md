# Eco2mix - Projet dbt

Projet d'analyse des données éco2mix avec dbt et Evidence.

## Prérequis

- Python 3.11+
- Node.js 20 (utiliser `nvm use` dans le dossier `reports/`)
- dbt-duckdb

## Installation

```bash
# Créer et activer l'environnement virtuel Python
python -m venv .venv
source .venv/bin/activate

# Installer les dépendances Python
pip install -r requirements.txt

# Installer les dépendances Evidence
cd reports
nvm use
npm install
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

## Evidence (Dashboard BI)

```bash
# Aller dans le dossier reports
cd reports

# Utiliser la bonne version de Node
nvm use

# Générer les sources de données
npx evidence sources

# Lancer le serveur de développement
npx evidence dev
```

L'interface Evidence sera accessible sur http://localhost:3000

## Structure du projet

```
├── models/           # Modèles dbt
│   ├── staging/      # Modèles de staging
│   └── intermediate/ # Modèles intermédiaires
├── reports/          # Dashboard Evidence
│   ├── pages/        # Pages du dashboard
│   └── sources/      # Sources de données
├── seeds/            # Données statiques
├── tests/            # Tests personnalisés
└── eco2mix.duckdb    # Base de données DuckDB
```
