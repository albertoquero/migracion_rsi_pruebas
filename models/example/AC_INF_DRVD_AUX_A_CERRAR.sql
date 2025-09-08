
with AC_INF_DRVD_AUX_A_CERRAR AS (
  SELECT
    A.entidad,
    A.acuerdo,
    A.codigo,
    A.fecha_inic,
    A.fecha_fin,
    A.valor
  FROM
    AQUERO_PRUEBAS_BRZ_DB.AQUERO_PRUEBAS.AC_INF_DRVD A
  INNER JOIN
    {{ ref('AC_INF_DRVD_AUX') }} AS AUX
    ON A.entidad = AUX.entidad
    AND A.acuerdo = AUX.acuerdo
    AND A.codigo = AUX.codigo
    AND A.valor != AUX.valor --cambio
  WHERE
    A.fecha_fin = CAST('9999-12-31' AS DATETIME)
) 

select * from AC_INF_DRVD_AUX_A_CERRAR