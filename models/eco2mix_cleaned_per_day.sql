{{ config(materialized='table') }}

SELECT * FROM {{ ref('int_eco2mix_cleaned_per_day') }}