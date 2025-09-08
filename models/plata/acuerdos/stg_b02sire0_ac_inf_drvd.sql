--SCD2  SIN MACROS

--materializar como incremental
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

  --materializar como tabla
WITH  
 
-- Excluir registros que ya existen exactamente igual en datos_actuales
nuevos_datos AS (
    SELECT nuevo.*
  FROM (
    SELECT * FROM {{ ref('base_b02sire0_inc0155') }}
    UNION ALL
    SELECT * FROM {{ ref('base_b02sire0_inc0156') }}
  ) nuevo
  LEFT JOIN datos_actuales actual
    ON actual.entidad = nuevo.entidad
   AND actual.acuerdo = nuevo.acuerdo
   AND actual.codigo = nuevo.codigo
   AND actual.valor = nuevo.valor
  WHERE actual.entidad IS NULL
),
datos_a_cerrar AS (
  SELECT
    actual.entidad,
    actual.acuerdo,
    actual.codigo,
    actual.fecha_inic,
    {{ fecha1 }} AS fecha_fin,
    actual.valor
  FROM {{this }} actual
  JOIN nuevos_datos nuevo
    ON actual.entidad = nuevo.entidad
    AND actual.acuerdo = nuevo.acuerdo
    AND actual.codigo = nuevo.codigo
    AND (actual.valor != nuevo.valor )
  WHERE actual.fecha_fin = CAST('9999-12-31' AS DATETIME)
),
datos_no_afectados AS (
  SELECT actual.entidad,
   actual.acuerdo,
    actual.codigo,
    actual.fecha_inic,
    actual.fecha_fin,
    actual.valor
  FROM {{this }} actual
  LEFT JOIN nuevos_datos nuevo
    ON nuevo.entidad = actual.entidad
   AND nuevo.acuerdo = actual.acuerdo
   AND nuevo.codigo = actual.codigo
  WHERE actual.fecha_fin != CAST('9999-12-31' AS DATETIME)
     OR nuevo.entidad IS NULL
)

SELECT * FROM nuevos_datos
UNION ALL
SELECT * FROM datos_a_cerrar
UNION ALL
SELECT * FROM datos_no_afectados