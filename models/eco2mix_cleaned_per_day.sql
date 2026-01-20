{{ config(materialized='table') }}

SELECT * FROM {{ ref('int_eco2mix_tco_tch_corrected') }}