WITH AC_INF_DRVD_AUX AS (
  SELECT
    entidad AS entidad,
    acuerdo ,
    codigo AS codigo,
    CAST('9999-12-31' AS DATETIME) AS fecha_fin,
    CAST('04/09/2025    ' AS DATETIME) AS fecha_inic,
    valor AS valor
  FROM
    AQUERO_PRUEBAS_BRZ_DB.AQUERO_PRUEBAS.ORIGEN_AC_INF_DRVD
)
select * from AC_INF_DRVD_AUX