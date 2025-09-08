with AC_INF_DRVD_AUX_FINAL AS (
  -- a) Nuevos registros que vienen de las tablas de origen
  SELECT
    AUX.entidad,
    AUX.acuerdo,
    AUX.codigo,
    AUX.fecha_inic,
    AUX.fecha_fin,
    AUX.valor
  FROM
    {{ ref('AC_INF_DRVD_AUX') }}   AUX
  UNION ALL
  -- b) Registros que deben ser cerrados, con fecha_fin la fecha de ejecución del proceso
  SELECT
    c.entidad,
    c.acuerdo,
    c.codigo,
    c.fecha_inic,
    CAST('04/09/2025' AS DATETIME) AS fecha_fin, 
    c.valor
  FROM
    {{ ref('AC_INF_DRVD_AUX_A_CERRAR') }}  c
)
select * from AC_INF_DRVD_AUX_FINAL