{{ config(
    materialized='view'
     ,schema='B02SIRE0'
     ,database='AQUERO_PRUEBAS_BRZ_DB' 
     
) }}
{% set fecha_inicio = var('fecha_inicio') %}
{% set fecha1 = "TO_DATE('" ~ fecha_inicio ~ "', 'DD/MM/YYYY')" %}

SELECT
    COD_NRBE_EN_W AS COD_NRBE_EN,
    CAST(NUM_SEC_AC_W AS NUMERIC) AS NUM_SEC_AC,
    COD_INF_DRVD_AC_W AS COD_INF_DRVD_AC,
    CAST('9999-12-31' AS DATETIME) AS MI_FECHA_FIN,
    CAST(FECHA_DATOS_W AS DATETIME) AS MI_FECHA_INIC, -- ver si hay que cambiar mi_fecha_inic por current_date o fecha_modificacion por otro valor
    CURRENT_DATE() AS FECHA_MODIFICACION,
    VALOR_INF_DRVD_W AS VALOR_INF_DRVD
  FROM
    {{ source('iris', 'STG_INC00156') }}
  WHERE
    FECHA_DATOS_W = {{ fecha1 }}