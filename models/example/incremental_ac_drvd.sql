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

{% set fecha1 = "TO_DATE('04/09/2025', 'DD/MM/YYYY')" %}

-- 1. Seleccionamos todos los registros nuevos de las tablas de origen, y les ponemos fecha_fin = 31-12-9999
WITH AC_INF_DRVD_AUX AS (
  SELECT
    entidad AS entidad,
    acuerdo ,
    codigo AS codigo,
    CAST('9999-12-31' AS DATETIME) AS fecha_fin,
    CAST('04/09/2025    ' AS DATETIME) AS fecha_inic,
    valor AS valor
  FROM
    ORIGEN_AC_INF_DRVD
),

-- 2. Identificamos los registros actuales que deben ser cerrados
-- (aquellos con fecha_fin = 31-12-9999 que tienen una nueva versión en los datos entrantes)
AC_INF_DRVD_AUX_A_CERRAR AS (
  SELECT
    A.entidad,
    A.acuerdo,
    A.codigo,
    A.fecha_inic,
    A.fecha_fin,
    A.valor
  FROM
     AC_INF_DRVD A
  INNER JOIN
    AC_INF_DRVD_AUX AS AUX
    ON A.entidad = AUX.entidad
    AND A.acuerdo = AUX.acuerdo
    AND A.codigo = AUX.codigo
    AND A.valor != AUX.valor
  WHERE
    A.fecha_fin = CAST('9999-12-31' AS DATETIME)
),
-- 3. Generamos los registros que vamos a insertar en la tabla final
AC_INF_DRVD_AUX_FINAL AS (
  -- a) Nuevos registros que vienen de las tablas de origen
  SELECT
    AUX.entidad,
    AUX.acuerdo,
    AUX.codigo,
    AUX.fecha_inic,
    AUX.fecha_fin,
    AUX.valor
  FROM
    AC_INF_DRVD_AUX AUX
  UNION ALL
  -- b) Registros que deben ser cerrados, con fecha_fin la fecha de ejecución del proceso
  SELECT
    c.entidad,
    c.acuerdo,
    c.codigo,
    c.fecha_inic,
    CAST({{ fecha1 }} AS DATETIME) AS fecha_fin, 
    c.valor
  FROM
    AC_INF_DRVD_AUX_A_CERRAR c
),

  -- c) Registros históricos que ya están cerrados y no necesitan actualización y registros abiertos que NO están afectados por los nuevos datos
AC_INF_DRVD_AUX_HIST AS (
  SELECT
    A.entidad,
    A.acuerdo,
    A.codigo,
    A.fecha_inic,
    A.fecha_fin,
    A.valor
  FROM
    AC_INF_DRVD A
  LEFT JOIN
    AC_INF_DRVD_AUX AS AUX -- Hacemos LEFT JOIN para encontrar los que NO están AFECTADOS por los cambios
    ON AUX.entidad = A.entidad
    AND AUX.acuerdo = A.acuerdo
    AND AUX.codigo = A.codigo
  WHERE
    AUX.entidad IS NULL -- Registros ya cerrados
),

AC_INF_DRVD_AUX_ALL AS (
-- Combinamos todos los sets de datos para la tabla final
  SELECT 
    entidad,
    acuerdo,
    codigo,
    fecha_inic,
    fecha_fin,
    valor 
  FROM AC_INF_DRVD_AUX_FINAL

  UNION DISTINCT

  SELECT
    entidad,
    acuerdo,
    codigo,
    fecha_inic,
    fecha_fin,
    valor  
  FROM AC_INF_DRVD_AUX_HIST
)

SELECT *
FROM AC_INF_DRVD_AUX_ALL