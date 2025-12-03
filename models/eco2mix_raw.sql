{{ config(materialized='table') }}

SELECT * FROM {{ source('parquet_file', 'eco2mix-regional-cons-def') }}