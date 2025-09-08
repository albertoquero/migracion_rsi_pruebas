{% macro scd2_gestion_fechas(
    modelos_origen,
    claves_primarias,
    campos_no_clave,
    fecha_inicio,
    fecha_fin_col='fecha_fin',
    fecha_inicio_col='fecha_inic'
) %}

 -- Limpia y prepara la lista de columnas clave primaria y las que no lo son
{% set pk_condicion = claves_primarias | map('trim') | list %} 
{% set valores_condicion = campos_no_clave | map('trim') | list %}

WITH 

-- CTE para leer los datos actuales de la tabla destino (modelo incremental)
datos_actuales AS (
  SELECT * FROM {{ this }}  
  {% if is_incremental() %}
    WHERE {{ fecha_fin_col }} = CAST('9999-12-31' AS DATETIME)
  {% endif %}
  
),

-- CTE que une todos los modelos base (nuevos datos) y filtra solo registros nuevos que no existen exactamente igual en destino
nuevos_datos AS (
  SELECT nuevo.*
  FROM (
    {% for model in modelos_origen %} -- Sacamos cada modelo base al que es referenciado
      SELECT * FROM {{ ref(model) }}  
      {% if not loop.last %} UNION ALL {% endif %}  -- Une todos con UNION ALL
    {% endfor %}
  )  nuevo
  LEFT JOIN datos_actuales actual
    ON
      {% for col in pk_condicion %}
        actual.{{ col }} = nuevo.{{ col }} AND  -- Igualdad en cada columna clave
      {% endfor %}
      {% for col in valores_condicion %}
        actual.{{ col }} = nuevo.{{ col }} {% if not loop.last %} AND {% endif %}
      {% endfor %}  -- Igualdad en columna de valor (para detectar cambios)
  WHERE actual.{{ pk_condicion[0] }} IS NULL  -- Nos quedamos solo con registros nuevos.
),

-- CTE para cerrar versiones anteriores que tienen cambios
datos_a_cerrar AS (
  SELECT
    {% for col in pk_condicion %}
      actual.{{ col }},  -- Claves primarias
    {% endfor %}
    actual.{{ fecha_inicio_col }},  -- Fecha inicio versión antigua
    TO_DATE('{{ fecha_inicio }}', 'YYYY-MM-DD') AS {{ fecha_fin_col }},  -- Fecha fin = fecha de carga actual
    {% for col in valores_condicion %}
      actual.{{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}  -- Valor antiguo que vamos a cerrar
  FROM datos_actuales actual
  JOIN nuevos_datos nuevo
    ON
      {% for col in pk_condicion %}
        actual.{{ col }} = nuevo.{{ col }} AND  -- Comprobacion de claves
      {% endfor %}
      (
      {% for col in valores_condicion %}
        actual.{{ col }} != nuevo.{{ col }}
        {% if not loop.last %} OR {% endif %}
      {% endfor %}
      ) -- Valor diferente: indica cambio, cerrar versión
  WHERE actual.{{ fecha_fin_col }} = CAST('9999-12-31' AS DATETIME)  -- Solo versiones abiertas
),

-- CTE para traer los registros que no se ven afectados (no hay cambios ni se deben cerrar)
datos_no_afectados AS (
  SELECT
    {% for col in pk_condicion %}
      actual.{{ col }},  -- Claves
    {% endfor %}
    actual.{{ fecha_inicio_col }},  -- Fecha inicio original
    actual.{{ fecha_fin_col }},  -- Fecha fin original
    {% for col in valores_condicion %}
      actual.{{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}  -- Valor original
  FROM datos_actuales actual
  LEFT JOIN nuevos_datos nuevo
    ON
      {% for col in pk_condicion %}
        actual.{{ col }} = nuevo.{{ col }} {% if not loop.last %} AND {% endif %}  -- Coinciden claves
      {% endfor %}
  WHERE actual.{{ fecha_fin_col }} != CAST('9999-12-31' AS DATETIME)  -- Versiones cerradas (fechas fin distintas)
     OR nuevo.{{ pk_condicion[0] }} IS NULL  -- O (es por entidad) que no fueron afectadas por nuevos datos
)

-- Finalmente combinamos todos los datos para el resultado final
SELECT * FROM nuevos_datos        -- Insertamos los nuevos registros abiertos
UNION ALL
SELECT * FROM datos_a_cerrar      -- Cerramos las versiones antiguas que cambiaron
UNION ALL
SELECT * FROM datos_no_afectados  -- Mantenemos las versiones sin cambios

{% endmacro %}