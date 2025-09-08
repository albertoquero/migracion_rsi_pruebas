{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['entidad', 'acuerdo', 'codigo', 'fecha_fin'],
    cluster_by = ['entidad'],
    partition_by={
      "field": "fecha_fin",
      "data_type": "DATETIME",
      "granularity": "day"
    }
  )
}}

SELECT *
FROM {{ ref('AC_INF_DRVD_AUX_ALL') }} 