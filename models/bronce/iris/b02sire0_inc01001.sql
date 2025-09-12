

{% set fecha1 = "TO_DATE('" ~ var('fecha_inicio') ~ "', 'DD/MM/YYYY')" %}

with source as (
    select * from {{ source('iris', 'STG_INC01001') }}
),
renamed as (
SELECT
		COD_NRBE_EN_W AS COD_NRBE_EN,
		COD_INTERNO_UO_W AS COD_INTERNO_UO,
		CAST(NUM_SEC_AC_W AS NUMERIC) AS NUM_SEC_AC,
		CURRENT_DATE() AS FECHA_MODIFICACION,
		COD_NRBE_EN_1_W AS COD_NRBE_EN_1,
		COD_INTERNO_UO_1_W AS COD_INTERNO_UO_1,
		CAST(NUM_SEC_AC_1_W AS NUMERIC) AS NUM_SEC_AC_1,
		COD_DIG_CR_UO_1_W AS COD_DIG_CR_UO_1,
		CAST(FECHA_OPRCN_W AS DATETIME) AS FECHA_OPRCN
	FROM
		source
	where
		TO_DATE(FECHA_DATOS_W,'DD/MM/YYYY') = {{ fecha1 }}
) 
select * from renamed