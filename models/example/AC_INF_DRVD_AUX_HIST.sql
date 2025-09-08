with AC_INF_DRVD_AUX_HIST AS (
  SELECT
    A.entidad,
    A.acuerdo,
    A.codigo,
    A.fecha_inic,
    A.fecha_fin,
    A.valor
  FROM
    AQUERO_PRUEBAS_BRZ_DB.AQUERO_PRUEBAS.AC_INF_DRVD A
  LEFT JOIN
    {{ ref('AC_INF_DRVD_AUX') }} AS AUX -- Hacemos LEFT JOIN para encontrar los que NO están AFECTADOS por los cambios
    ON AUX.entidad = A.entidad
    AND AUX.acuerdo = A.acuerdo
    AND AUX.codigo = A.codigo
  WHERE
    AUX.entidad IS NULL -- Registros ya cerrados
)
 select * from AC_INF_DRVD_AUX_HIST