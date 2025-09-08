
{% set fecha1 = "TO_DATE('04/09/2025', 'DD/MM/YYYY')" %}

with AC_INF_DRVD_AUX_ALL AS (
-- Combinamos todos los sets de datos para la tabla final
  SELECT 
    entidad,
    acuerdo,
    codigo,
    fecha_inic,
    fecha_fin,
    valor 
  FROM {{ ref('AC_INF_DRVD_AUX_FINAL') }} 

  UNION DISTINCT

  SELECT
    entidad,
    acuerdo,
    codigo,
    fecha_inic,
    fecha_fin,
    valor  
  FROM {{ ref('AC_INF_DRVD_AUX_HIST') }}  
)

SELECT *
FROM AC_INF_DRVD_AUX_ALL