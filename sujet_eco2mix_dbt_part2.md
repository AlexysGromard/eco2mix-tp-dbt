# E(L)T, Datawarehouse, OLAP et SQL avancé (Partie 2)

A Modern Data Stack with `dbt`, `DuckDB` (and `Evidence`) !

---

## Rappel du contexte

Il s'agit de la seconde partie d'un TP en deux volets. La première partie, disponible séparément, porte sur la prise en main de `DuckDB` et l'exploration initiale du jeu de données éCO2mix régional consolidé et définitif (de janvier 2013 à décembre 2024).

Dans cette seconde partie, vous allez mettre en place un projet `dbt-core` pour transformer les données extraites dans la première partie, et construire un entrepôt de données (MDS) selon un schéma en étoile. Vous allez également exploiter des fonctionnalités SQL avancées, en particulier les fonctions de fenêtrage, les CTE récursives et la construction de cubes OLAP.

### Outils et technologies

SQL, DuckDB, dbt-core, Jinja, Evidence, Markdown

- la documentation de [DuckDB](https://duckdb.org/docs/)
- la documentation de [dbt](https://docs.getdbt.com/docs/introduction), sélectionner `Core V1.10`
- un site dédié à [SQL](https://sql.sh)
- la documentation d'[Evidence](https://docs.evidence.dev)
- le guide de [Jinja](https://jinja.palletsprojects.com/en/stable/templates/)


### dbt, à quoi ça sert ?!

`dbt` (_data build tool_) est un outil de transformation de données (le __T__ de ETL) pour alimenter un entrepôt à partir de sources opérationnelles.  

Chaque _transformation_ `dbt`, appelé __modèle__, est définie exclusivement à l'aide d'une __requête SQL__ ! Par exemple, la requête

```sql
-- contenu du fichier models/example/my_model.sql
SELECT A as AAA, B FROM source_table WHERE C IS NOT NULL
```

définit un modèle `dbt` qui construit la vue `my_model(AAA, B)` par extraction des colonnes `A` et `B` de la table `source_table`, en filtrant les lignes où la colonne `C` est non nulle.

__!! Attention !!__ `dbt` est disponible dans plusieurs éditions: `dbt-core`, `dbt` _Fusion engine_, `dbt` _platform_, et même `dbt` _Cloud CLI_. Seule l'édition `dbt-core` (commande `dbt` à partir d'une fenêtre de `Terminal`) est utilisée dans ce TP. En particulier, l'installation de l'extension officielle dbt pour VS Code, qui repose sur le moteur `dbt` _Fusion_, n'est pas prise en charge.

### Livrable

Un projet `dbt` intégral, livré sous la forme d'une simple URL de dépôt <gitlab.univ-nantes.fr> dans lequel figurent des modèles de données (définis par des transformations), des tests et de la documentation, plus tous les objets utiles au projet. Le code doit pouvoir être rejoué sans erreur, du début à la fin. Le fichier `README.md`, à la racine du projet, doit livrer les instructions pour la mise en route du projet, et doit expliquer les choix réalisés.

Si d'autres outils sont utilisés au cours du projet (`Evidence` pour la présentation des résultats d'analyse, par exemple), il est judicieux de constituer un `monorepo` à partir du projet `dbt`.

---

## 0. Mise en place de l'environnement de travail

### éco-système Python, dbt et DuckDB

Dans le Terminal, créer un répertoire racine `<OLAP_PRJ>` (nom quelconque, à choisir) pour tout le TP (données, projet dbt, etc.) et y installer un environnement virtuel Python avec les packages `dbt-core` et `dbt-duckdb`.

```bash
~/$ mkdir <OLAP_PRJ>
~/$ cd <OLAP_PRJ>
<OLAP_PRJ>/$ python -m venv venv
<OLAP_PRJ>/$ source venv/bin/activate
<OLAP_PRJ>/$ pip install dbt-core dbt-duckdb
```

Si le tuyau est bouché, penser au proxy:

```bash
~/$ export HTTPS_PROXY=http://proxy-etu.polytech.univ-nantes.prive:3128/
```

### Démarrer un projet `dbt`

Initialiser un projet dbt dans le répertoire `<OLAP_PRJ>/eco2mix/` avec la commande :

```bash
<OLAP_PRJ>/$ dbt init eco2mix
```

Observer la structure du projet `dbt` :

```bash
<OLAP_PRJ>/$ tree eco2mix
```

Puis exécuter le projet dans sa version initiale :

```bash
<OLAP_PRJ>/$ cd eco2mix
<OLAP_PRJ>/eco2mix/$ dbt run
```

Observer les requêtes SQL, définies dans le répertoire `models/example/`, compilées dans le répertoire `target/compiled/` et préparées dans `target/run/`. En outre, un nouveau fichier `dev.duckdb` a été créé, qui contient la base de données DuckDB produite par les transformations. Vous pouvez explorer cette base de données avec le client `duckdb` (ou sa version graphique avec l'option `-ui`) :

```bash
<OLAP_PRJ>/eco2mix/$ duckdb dev.duckdb
D .tables
```

Pour jouer les tests de votre projet `dbt` :

```bash
<OLAP_PRJ>/eco2mix/$ dbt test
```

Et pour générer et parcourir la documentation :

```bash
<OLAP_PRJ>/eco2mix/$ dbt docs generate
<OLAP_PRJ>/eco2mix/$ dbt docs serve
```

La configuration de la connexion à la base DuckDB à partir de `dbt` se fait dans le fichier `profiles.yml`, situé dans le répertoire `~/.dbt/`.
Les options de configuration sont détaillées dans le README du package `dbt-duckdb` : <https://github.com/duckdb/dbt-duckdb>. Une version mininale et (vraisemblablement) suffisante pour ce TP est la suivante :

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

La prise en main de `dbt-core` passe par la lecture de la [documentation](https://docs.getdbt.com/docs/introduction). Un scénario complet d'utilisation de `dbt` avec `DuckDB` est disponible ici : <https://docs.getdbt.com/guides/duckdb>, pour information.

_Tips_ : il est recommandé de versionner son projet `dbt` avec `git`.

---

## 1. Extraction et premiers pas avec le jeu de données

### Brancher la source de données

Le fichier `parquet` original doit être connecté en tant que __source__ dans le projet `dbt`.

La création d'un fichier `sources.yml` dans le répertoire `models/` permet de déclarer la source de données. Par exemple :

```yaml
sources:
   - name: parquet_file
    meta:
      external_location: "path/to/{name}.parquet"
    tables:
      - name: eco2mix-regional-cons-def
      - name: eco2mix-regional-tr
      - name: temperature-quotidienne-regionale
```

Le champ `external_location` indique le chemin relatif vers le fichier `parquet`. Le `{name}` est un _placeholder_ qui sera remplacé par le nom de la table, tel que défini dans la section `tables`.
Avec une déclaration de source comme celle-ci, le modèle `dbt` peut référencer la table source grâce à la syntaxe :

```sql
SELECT * FROM {{ source('parquet_file', 'eco2mix-regional-cons-def') }}
```

La notation `{{ ... }}` est une construction de _templating_ `Jinja`, qui permet d'injecter du code dynamique dans les modèles `dbt`. La documentation de `dbt` explique comment utiliser `Jinja` : <https://docs.getdbt.com/docs/build/jinja-macros>.

---

## 2. Nettoyage des données

Reprendre et compléter les étapes de nettoyage et de préparation des données vues dans la première partie du TP et les formaliser comme modèles (transformations) dans le projet `dbt`.

Penser à documenter (champ _description_) les modèles ainsi créés, et proposer des _tests_ simples pour vérifier la qualité des données.

Considérer également la construction d'une vue matérialisée (ou d'une table) dont le grain est journalier, pour faciliter les analyses ultérieures. La stratégie de matérialisation des modèles `dbt` est définie dans le fichier `dbt_project.yml` et peut être redéfinie au niveau de chaque modèle (cf. documentation : <https://docs.getdbt.com/docs/build/materializations>).

---

## 3. Exploration

Écrire les requêtes SQL pour traiter les problèmes exposés ci-dessous, avec éventuellement un rendu visuel (graphique) via `Evidence`. Tout _embellissement_ du résultat, dans la mesure où il améliore effectivement la lisibilité, sera apprécié (et donc valorisé).

Le résultat de chaque requête doit faire l'objet d'une brève interprétation textuelle. Si nécessaire, un complément d'investigation (une ou plusieurs requêtes supplémentaires, qui permettent une interprétation plus fine) peut être proposé pour approfondir un sujet.

### Bref détour par `Evidence`

Il s'agit d'un outil d'aide à la décision, qui permet de présenter des résultats d'analyse sous forme de tableaux et de graphiques, à partir de requêtes SQL. La sortie est diffusée sous forme de pages webs interactives,l construites à partir de modèles `Markdown` enrichis de blocs SQL.

`Evidence` complète parfaitement un projet `dbt`, en permettant de présenter les résultats d'analyse issus des modèles `dbt` (ou d'autres sources de données).
Pour installer `Evidence`, dans le terminal, exécuter :

```bash
cd path/to/your/dbt/project
npx degit evidence-dev/template reports
npm --prefix ./reports install
npm --prefix ./reports run sources
npm --prefix ./reports run dev
```

`Evidence` requiert une installation de NodeJS, comme spécifié dans la documentation : <https://docs.evidence.dev/guides/system-requirements/>.

Un tutoriel d'utilisation d'`Evidence` est consultable ici : <https://docs.evidence.dev/build-your-first-app/>.

Par la suite, la documentation intégrale permet de progresser dans la maîtrise de l'outil : <https://docs.evidence.dev>.

__!! Attention !!__ : Comme pour `dbt`, `Evidence` se décline en plusieurs éditions: CLI, Evidence Studio et extension VSCode. Seule la version CLI est utilisée dans ce TP.

### Les questions à traiter

Chaque requête SQL peut être implémentée en tant qu'_analyse_ dans le projet `dbt` (répertoire `analyses/`), pour bénéficier de la gestion de version, du moteur de _templating_ et de la reproductibilité offertes par `dbt`. Ensuite, il s'agit de copier-coller chaque requête SQL compilée (dans le répertoire `target/compiled/`) dans un bloc SQL d'un fichier `Markdown` d'`Evidence`, pour produire le rendu visuel.

1. __Groupement et agrégation simples__ : Production (en GWh) et consommation (en GWh), ainsi que leurs versions min, max et moyenne instantanées (en Mw), par mois et par région.

2. __Pivot__ (construction `CASE WHEN THEN END`) : consommation (GWh) journalière, détaillée par région. Par construction, chaque date devient une clé de la relation-résultat. Autrement dit, il y a une ligne de données de consommation pour chaque date et une colonne par région.

3. __Fenêtre glissante__ (_window function_ avec `RANGE`) : la consommation régionale (GWh) du mois écoulé, chaque jour. Par exemple, pour le 15 mars 2024, la consommation cumulée du 15 février au 15 mars 2024. Le résultat doit comporter une ligne par jour de la période considérée (2013-2024) et par région.

4. __Variation__ (_window function_, avec _CTE_ pour décomposer le calcul) : les 20 plus grands écarts de consommation quotidienne (GWh), d'un jour à l'autre, toutes régions confondues.

5. __Quantité cumulée__ (_window functions_ + _CTE_) : jour du dépassement des énergies renouvelables, pour chaque année (de 2013 à 2024). En d'autres termes, à quel moment de l'année (une date) la consommation atteint - dépassse - la production annuelle totale des filières renouvelables ?

6. __Calcul de point fixe__ (_CTE récursive_) : trouver toutes les périodes correspondant aux 3 plus longues séquences d'augmentation de la consommation instantanée. Voici un extrait (la première ligne) de résultat escompté :
| Date - Heure        | Durée (hh:mm:ss) | Région       | Séquence (MW*)                                   | Rang |
| :-----------        | :---             | ---          | ---                                              | ---  |
| 2016-07-18 02:30:00 | 11:00:00         | Île-de-France| [4616, 4646, 4661, 4715, 4942, 5009, 5391, 568...| 1    |

7. __Construction du cube__ (`GROUP BY CUBE|GROUPING SETS|ROLLUP`) : donner toutes les valeurs de consommation (en GWh) agrégés par jour, par mois, par année et sur toute la période, ainsi que par région, par zone (NO, NE, SO, SE et IdF) et sur l'ensemble du territoire métropolitain (à l'exclusion de la Corse, non représentée dans le jeu de données). Toutes les combinaisons de ces 2 dimensions (temps et géographie) doivent figurer dans le résultat.

---

## 4. De la zone de transit à l'entrepôt

Il est maintenant temps de concevoir et implémenter une base de données proprement multi-dimensionnelle, un entrepôt, selon un _schéma en étoile_.

1. Intégrer un second un jeu de données d'historique des températures (2016 à 2024), le [relevé des températures quotidiennes régionales (depuis janvier 2016)](https://odre.opendatasoft.com/explore/dataset/temperature-quotidienne-regionale/), disponible sur le [portail ODRÉ](https://opendata.reseaux-energies.fr). Le fichier `parquet` est disponible sur [uncloud](https://uncloud.univ-nantes.fr/public.php/dav/files/WHYMCmFrJ2FTaa9/?accept=zip).
Résoudre les problèmes d'alignement/jointure, sur la base d'un relevé par jour. Conserver les températures minimale, maximale et moyenne.

2. Concevoir, implémenter et alimenter avec les données existantes, une base de données multi-dimensionnelle comportant les mesures de production et de consommation régionale quotidienne (GWh), de production régionale quotidienne par filière (GWh), ainsi que les taux de couverture (TCO), sur les trois dimensions : temps (jour, mois, saison, année, toute la période), géographie (région, quarts, pays), température (au degré, par intervalle (glacial=$[-\infty,0)$, froid=$[0,8)$, modéré=$[8,17)$, idéal=$[17,25]$, chaud=$[25,33)$, extrême=$[33,+\infty)$), et toutes températures confondues).

3. Proposer la requête sur ce schéma en étoile, qui permette de construire le __cuboïde__ (la vue) par mois, par quart et par intervalle de température.

4. Proposer une procédure pour la mise-à-jour incrémentale (sans recalculer l'intégralité) de l'entrepôt lorsque de nouvelles données sont disponibles. Utiliser pour l'expérience, le jeu de données complémentaire [éCO2mix régional temps réel](https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-tr/). Envisager le changement de statut de _donnée temps réel_ à _donnée consolidée_ puis _données définitives_.

---

## 5. [Bonus] Étude comparative des formats de stockage

1. Proposer une évaluation de performance (taille et temps de réponse) pour la lecture (intégrale ou par requête décisionnelle) et l'écriture de données de/vers `CSV`, `JSON`, `PARQUET` (compression `SNAPPY` ou `ZSTD`) et DuckDB.

2. Proposer une évaluation de performance du format `Apache Arrow` dans DuckDB. Envisager pour cela le _driver `ADBC`_.

---

## Ressources supplémentaires et liens (peut-être) utiles

- exemple de MDS DuckDB + dbt : <https://motherduck.com/blog/duckdb-dbt-e2e-data-engineering-project-part-2/>
- dépôt de packages pour dbt : <https://hub.getdbt.com>
- guide de démmarage rapide avec dbt core : <https://docs.getdbt.com/guides/manual-install>
- langage de templating Ninja pour dbt : <https://docs.getdbt.com/guides/using-jinja>
- documentation CLI duckdb : <https://duckdb.org/docs/api/cli/overview.html>