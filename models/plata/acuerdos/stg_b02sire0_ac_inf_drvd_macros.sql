--SCD2  CON MACROS


 {{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['entidad', 'acuerdo', 'codigo', 'fecha_fin'],
    cluster_by=['entidad'],
    partition_by={
      "field": "fecha_fin",
      "data_type": "DATETIME",
      "granularity": "day"
    }
) }}

 
{% set fecha_inicio = var('fecha_inicio') %}

{{ scd2_gestion_fechas(
    modelos_origen=['base_b02sire0_inc0155', 'base_b02sire0_inc0156'],
    claves_primarias=['entidad', 'acuerdo', 'codigo'],
    campos_no_clave=['valor'],
    fecha_inicio=fecha_inicio
) }} 

 