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
{% set fecha1 = "TO_DATE('" ~ fecha_inicio ~ "', 'YYYY-MM-DD')" %}



WITH nuevos_datos AS (
  SELECT
    COD_NRBE_EN_W AS COD_NRBE_EN,
    CAST(NUM_SEC_AC_W AS NUMERIC) AS NUM_SEC_AC,
    COD_INF_DRVD_AC_W AS COD_INF_DRVD_AC,
    CAST('9999-12-31' AS DATETIME) AS MI_FECHA_FIN,
    CAST(FECHA_DATOS_W AS DATETIME) AS MI_FECHA_INIC,
    CURRENT_DATETIME() AS FECHA_MODIFICACION,
    VALOR_INF_DRVD_W AS VALOR_INF_DRVD
  FROM
       {{ source('AQUERO_PRUEBAS_BRZ_DB', 'ORIGEN_AC_INF_DRVD_STG0156') }}
  WHERE
    FECHA_DATOS_W = {{ fecha1 }}
    UNION ALL
    SELECT
    COD_NRBE_EN_W AS COD_NRBE_EN,
    CAST(NUM_SEC_AC_W AS NUMERIC) AS NUM_SEC_AC,
    COD_INF_DRVD_AC_W AS COD_INF_DRVD_AC,
    CAST('9999-12-31' AS DATETIME) AS MI_FECHA_FIN,
    CAST(FECHA_DATOS_W AS DATETIME) AS MI_FECHA_INIC,
    CURRENT_DATETIME() AS FECHA_MODIFICACION,
    VALOR_INF_DRVD_W AS VALOR_INF_DRVD
  FROM
      {{ source('AQUERO_PRUEBAS_BRZ_DB', 'ORIGEN_AC_INF_DRVD_STG0156') }}
  WHERE
    FECHA_DATOS_W = {{ fecha1 }}

),
datos_a_cerrar AS (
  SELECT
    actual.entidad,
    actual.acuerdo,
    actual.codigo,
    actual.fecha_inic,
    {{ fecha1 }} AS fecha_fin,
    actual.valor
  FROM {{ this }} actual
  JOIN nuevos_datos nuevo
    ON actual.entidad = nuevo.entidad
    AND actual.acuerdo = nuevo.acuerdo
    AND actual.codigo = nuevo.codigo
    AND actual.valor != nuevo.valor
  WHERE actual.fecha_fin = CAST('9999-12-31' AS DATETIME)
),
datos_no_afectados AS (
  SELECT *
  FROM {{ this }} 
  WHERE fecha_fin != CAST('9999-12-31' AS DATETIME)
    OR NOT EXISTS (
      SELECT 1
      FROM nuevos_datos nuevo
      WHERE nuevo.entidad = AC_INF_DRVD.entidad
        AND nuevo.acuerdo = AC_INF_DRVD.acuerdo
        AND nuevo.codigo = AC_INF_DRVD.codigo
    )
)

SELECT * FROM nuevos_datos
UNION ALL
SELECT * FROM datos_a_cerrar
UNION ALL
SELECT * FROM datos_no_afectados
