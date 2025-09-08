{{ config(
    materialized='ephemeral'
) }}
{% set fecha_inicio = var('fecha_inicio') %}
{% set fecha1 = "TO_DATE('" ~ fecha_inicio ~ "', 'YYYY-MM-DD')" %}

SELECT
    ENTIDAD AS ENTIDAD,
    CAST(ACUERDO AS STRING) AS ACUERDO,
    CODIGO AS CODIGO,
    CAST(FECHA_FICHERO AS DATETIME) AS FECHA_INIC,
    CAST('9999-12-31' AS DATETIME) AS FECHA_FIN,
    
    VALOR AS VALOR
  FROM
    {{ source('AQUERO_PRUEBAS_BRZ_DB', 'ORIGEN_AC_INF_DRVD_STG0155') }}
  WHERE
    FECHA_FICHERO = {{ fecha1 }}