---
title: Eco2mix - Production d'électricité en France
---

# 🔌 Dashboard Eco2mix

Ce dashboard présente les données de production et consommation d'électricité en France par région.

## Production par source d'énergie

```sql production_totale
SELECT 
    SUM(consommation) as consommation_totale,
    SUM(nucleaire) as nucleaire,
    SUM(thermique) as thermique,
    SUM(hydraulique) as hydraulique,
    SUM(eolien) as eolien,
    SUM(solaire) as solaire,
    SUM(bioenergies) as bioenergies
FROM eco2mix.eco2mix_data
```

<BigValue 
    data={production_totale} 
    value=consommation_totale 
    title="Consommation totale (MW)"
    fmt="num0"
/>

<BigValue 
    data={production_totale} 
    value=nucleaire 
    title="Nucléaire (MW)"
    fmt="num0"
/>

<BigValue 
    data={production_totale} 
    value=eolien 
    title="Éolien (MW)"
    fmt="num0"
/>

<BigValue 
    data={production_totale} 
    value=solaire 
    title="Solaire (MW)"
    fmt="num0"
/>

## Consommation par région

```sql conso_par_region
SELECT 
    libelle_region,
    SUM(consommation) as consommation
FROM eco2mix.eco2mix_data
WHERE libelle_region IS NOT NULL
GROUP BY libelle_region
ORDER BY consommation DESC
```

<BarChart
    data={conso_par_region}
    x=libelle_region
    y=consommation
    title="Consommation par région (MW)"
/>

## Mix énergétique

```sql mix_energetique
SELECT 
    'Nucléaire' as source, SUM(nucleaire) as production FROM eco2mix.eco2mix_data
UNION ALL
SELECT 'Thermique', SUM(thermique) FROM eco2mix.eco2mix_data
UNION ALL
SELECT 'Hydraulique', SUM(hydraulique) FROM eco2mix.eco2mix_data
UNION ALL
SELECT 'Éolien', SUM(eolien) FROM eco2mix.eco2mix_data
UNION ALL
SELECT 'Solaire', SUM(solaire) FROM eco2mix.eco2mix_data
UNION ALL
SELECT 'Bioénergies', SUM(bioenergies) FROM eco2mix.eco2mix_cleaned
```

<BarChart
    data={mix_energetique}
    x=source
    y=production
    title="Répartition du mix énergétique"
    swapXY=true
/>

## Aperçu des données

```sql apercu
SELECT * FROM eco2mix.eco2mix_data LIMIT 100
```

<DataTable data={apercu} rows=10 />

- Deploy your project with [Evidence Cloud](https://evidence.dev/cloud)

## Get Support
- Message us on [Slack](https://slack.evidence.dev/)
- Read the [Docs](https://docs.evidence.dev/)
- Open an issue on [Github](https://github.com/evidence-dev/evidence)
