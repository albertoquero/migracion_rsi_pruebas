
 {{ config(
    unique_key=['COD_NRBE_EN', 'COD_INTERNO_UO', 'NUM_SEC_AC'],
    cluster_by=['COD_NRBE_EN'],
) }}


{{ scd2_gestion_fechas(
    modelos_origen=['b02sire0_inc01001' ],
    claves_primarias=['COD_NRBE_EN', 'COD_INTERNO_UO', 'NUM_SEC_AC'],
    campos_no_clave=['COD_NRBE_EN_1','COD_INTERNO_UO_1','NUM_SEC_AC_1','COD_DIG_CR_UO_1','FECHA_OPRCN'],
    fecha_inicio=fecha_inicio
) }} 


WITH AC_CUENTA_NUEVA_AUX AS (
SELECT
	A.COD_NRBE_EN,
	A.COD_INTERNO_UO,
	A.NUM_SEC_AC,
	A.FECHA_MODIFICACION,
	A.COD_NRBE_EN_1,
	A.COD_INTERNO_UO_1,
	A.NUM_SEC_AC_1,
	A.COD_DIG_CR_UO_1,
	A.FECHA_OPRCN
FROM
	{{ ref("b02sire0_inc01001") }} A
LEFT JOIN 
    {{ this }} B 
    ON A.COD_NRBE_EN = B.COD_NRBE_EN
	AND A.COD_INTERNO_UO = B.COD_INTERNO_UO
	AND A.NUM_SEC_AC = B.NUM_SEC_AC
WHERE
	B.COD_NRBE_EN IS NULL
)

SELECT *
FROM AC_CUENTA_NUEVA_AUX