
 {{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['COD_NRBE_EN', 'NUM_SEC_AC', 'COD_INF_DRVD_AC', 'MI_FECHA_FIN'],
    cluster_by=['COD_NRBE_EN'],
    partition_by={
      "field": "MI_FECHA_FIN",
      "data_type": "DATETIME",
      "granularity": "day"
    },
     schema='SIR'
     ,database='AQUERO_PRUEBAS_SLVR_DB' 

) }}

 
{% set fecha_inicio = var('fecha_inicio') %}

{{ scd2_gestion_fechas(
    modelos_origen=['b02sire0_inc0155', 'b02sire0_inc0156'],
    claves_primarias=['COD_NRBE_EN', 'NUM_SEC_AC', 'COD_INF_DRVD_AC'],
    campos_no_clave=['VALOR_INF_DRVD'],
    fecha_inicio=fecha_inicio
) }} 