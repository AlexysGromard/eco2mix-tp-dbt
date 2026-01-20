-- Q2: Consommation (GWh) journalière, détaillée par région (pivot)

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
