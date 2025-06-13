# Importación de librerias y variables
import pandas as pd
from limpieza_concatenacion import insumo_consultas
from Exclusive_CxS_functions import Reemplazos_multiples_columnas
from Trasformation_functions import (
    Reemplazar_columna_en_funcion_de_otra,
)

# Importar constantes adicionales.
from config_constans import (
    CONSULTA_DS,
    CONSULTA_NO_DS,
    CONSULTA_CADENAS,
    CONCATENADA,
    COLS_CNAL_SUBCNAL_SEG,
    FIL_SIN_ASIGNAR,
    SEGMENTO_AGRUP,
    SEGMENTO_TRANS,
    COLS_CONCAT_CADENAS,
    RAMO_CLAVE,
    FORMATO,
    FORMATO_NIF,
    FIL_NUMERAL,
    REEMPLAZOS,
    COLS_CNAL_SUBCNAL_SEG_FNIF,
    COLS_CNAL_SUBCNAL_SEG_F,
)

from config_dicts import (
    DICT_SEG_AGRUP_NO_DS,
    DICT_REMP_CNAL_SUBCNAL_SEG_NO_DS,
    DICT_SEG_AGRUP_CADENAS,
    DICT_CLIENTE_SIN_ASIGNAR_FALTANTES,
    DICT_CLIENTE_FORMATO_FALTANTES,
    DICT_REMP_CNAL_SUBC_SEG_MAL_CREADOS,
    DICT_REMP_CNAL_SUBC_SEG_FOR_FORNIF_CADENAS_NK,
)


# Trasformaciones de este Script.
# 0. Remplazar algunos valores en todos los Dataframes.
# 1. Actualización valores "sin asignar" en consulta_no_ds"
# 2. Agregar Columna segmento agrupado a todas las consultas.
# 3. Modificar Col Segmento agrupado en consulta_no_ds
# 4. Modificar Col Segmento agrupado en consulta_cadenas.
consultas_mod = insumo_consultas.copy()
# 0.
# Iterar a través de la lista de reemplazos y aplicarlos
for reemplazo_info in REEMPLAZOS:
    df = consultas_mod[reemplazo_info["df"]]
    columna = reemplazo_info["columna"]
    reemplazo = reemplazo_info["reemplazo"]
    nuevo_valor = reemplazo_info["nuevo_valor"]

    df[columna] = df[columna].replace(reemplazo, nuevo_valor)

# Caso particular que tiene ortografia.
consultas_mod[CONSULTA_CADENAS][FORMATO] = consultas_mod[CONSULTA_CADENAS][FORMATO].replace(
    "ALMACENES EXITO S.A.", "EXITO"
)

# 1.
# Actualización de valores "sin asignar" canal trasformado, sub_canal_transformado, segmento_trasformado.
# Aplicar la función a cada DataFrame en el diccionario de la llave consultas_no_ds.
consultas_mod[CONSULTA_NO_DS] = Reemplazos_multiples_columnas(
    df=consultas_mod[CONSULTA_NO_DS],
    columnas_a_verificar=COLS_CNAL_SUBCNAL_SEG,
    reemplazos=DICT_REMP_CNAL_SUBCNAL_SEG_NO_DS,
    nom_condicion=FIL_SIN_ASIGNAR,
    col_clave=RAMO_CLAVE,
)

# 2.
# Agregar columna segmento agrupado a ambos dataframes ds y no_ds.
consultas_mod[CONSULTA_DS][SEGMENTO_AGRUP] = FIL_SIN_ASIGNAR
consultas_mod[CONSULTA_NO_DS][SEGMENTO_AGRUP] = FIL_SIN_ASIGNAR
consultas_mod[CONSULTA_CADENAS][SEGMENTO_AGRUP] = FIL_SIN_ASIGNAR

# 3.
# Llamada a la función para realizar los reemplazos
consultas_mod[CONSULTA_NO_DS] = Reemplazar_columna_en_funcion_de_otra(
    df=consultas_mod[CONSULTA_NO_DS],
    nom_columna_a_reemplazar=SEGMENTO_AGRUP,
    nom_columna_de_referencia=SEGMENTO_TRANS,
    mapeo=DICT_SEG_AGRUP_NO_DS,
)

# 4
# Utilizar el método str.cat() para concatenar los valores de las columnas sin guiones
# Agregar una columna temporal al dataframe para hacer las comparaciones.
# Variable que contiene la lista de cadenas a concatenar para la "union"


# Agregar una columna temporal "concatenada" al dataframe para hacer las comparaciones.
consultas_mod[CONSULTA_CADENAS][CONCATENADA] = consultas_mod[CONSULTA_CADENAS][COLS_CONCAT_CADENAS[0]].str.cat(
    [consultas_mod[CONSULTA_CADENAS][COLS_CONCAT_CADENAS[1]], consultas_mod[CONSULTA_CADENAS][COLS_CONCAT_CADENAS[2]]], sep=""
)

condicion_general = (
    consultas_mod[CONSULTA_CADENAS][COLS_CNAL_SUBCNAL_SEG] == FIL_SIN_ASIGNAR
).all(axis=1)


for valor_columna, remplazo in DICT_REMP_CNAL_SUBC_SEG_FOR_FORNIF_CADENAS_NK.items():
    condicion = (
        condicion_general 
        & (consultas_mod[CONSULTA_CADENAS][RAMO_CLAVE]== valor_columna)
        & (consultas_mod[CONSULTA_CADENAS][FORMATO] == FIL_SIN_ASIGNAR)
        & (consultas_mod[CONSULTA_CADENAS][FORMATO_NIF] == FIL_NUMERAL)
    )

    indices_a_remplazar = consultas_mod[CONSULTA_CADENAS][condicion].index

    consultas_mod[CONSULTA_CADENAS].loc[
        indices_a_remplazar, COLS_CNAL_SUBCNAL_SEG_F
    ] = remplazo
    
    consultas_mod[CONSULTA_CADENAS].loc[
        indices_a_remplazar, FORMATO_NIF
    ] = FIL_SIN_ASIGNAR
    
for valor_columna, remplazo in DICT_REMP_CNAL_SUBC_SEG_MAL_CREADOS.items():
    condicion = (
        condicion_general
        & (consultas_mod[CONSULTA_CADENAS][FORMATO] == valor_columna)
        & (consultas_mod[CONSULTA_CADENAS][FORMATO_NIF] != FIL_NUMERAL)
        & (consultas_mod[CONSULTA_CADENAS][FORMATO_NIF] != FIL_SIN_ASIGNAR)
    )

    indices_a_remplazar = consultas_mod[CONSULTA_CADENAS][condicion].index

    consultas_mod[CONSULTA_CADENAS].loc[
        indices_a_remplazar, COLS_CNAL_SUBCNAL_SEG
    ] = remplazo


for valor_columna, remplazo in DICT_CLIENTE_FORMATO_FALTANTES.items():
    condicion = (
        (condicion_general)
        & (consultas_mod[CONSULTA_CADENAS][FORMATO] == valor_columna)
        & (consultas_mod[CONSULTA_CADENAS][FORMATO_NIF] != FIL_SIN_ASIGNAR)
        & (consultas_mod[CONSULTA_CADENAS][FORMATO_NIF] == FIL_NUMERAL)
    )

    indices_a_remplazar = consultas_mod[CONSULTA_CADENAS][condicion].index

    consultas_mod[CONSULTA_CADENAS].loc[
        indices_a_remplazar, COLS_CNAL_SUBCNAL_SEG_FNIF
    ] = remplazo

for valor_columna, remplazo in DICT_CLIENTE_SIN_ASIGNAR_FALTANTES.items():
    condicion = (
        (
            consultas_mod[CONSULTA_CADENAS][COLS_CNAL_SUBCNAL_SEG_F] == FIL_SIN_ASIGNAR
        ).all(axis=1)
        & (consultas_mod[CONSULTA_CADENAS][RAMO_CLAVE] == valor_columna)
        & (consultas_mod[CONSULTA_CADENAS][FORMATO_NIF] != FIL_NUMERAL)
    )
    indices_a_remplazar = consultas_mod[CONSULTA_CADENAS][condicion].index

    consultas_mod[CONSULTA_CADENAS].loc[
        indices_a_remplazar, COLS_CNAL_SUBCNAL_SEG
    ] = remplazo


# Modificar la columna Segmento Agrupado para cadenas.
# Llamada a la función para realizar los reemplazos
consultas_mod[CONSULTA_CADENAS] = Reemplazar_columna_en_funcion_de_otra(
    df=consultas_mod[CONSULTA_CADENAS],
    nom_columna_a_reemplazar=SEGMENTO_AGRUP,
    nom_columna_de_referencia=CONCATENADA,
    mapeo=DICT_SEG_AGRUP_CADENAS,
)
#Lo que no haya quedado remplazado, es decir : Seg_Agrup == "Sin asignar"
#Reemplazar con el driver de segmento agrupado de DS y NO_DS. 
# Remplazar el Segmento Agrupado. igual que como hicimos en la consulta
# no_ds == sin clientes y en la consulta ds
mask2 = consultas_mod[CONSULTA_CADENAS][SEGMENTO_AGRUP] == FIL_SIN_ASIGNAR 

subconjunto_cadenas=consultas_mod[CONSULTA_CADENAS].loc[mask2, :].copy()
# Aplicar la función solo en las filas seleccionadas por la máscara
consultas_mod[CONSULTA_CADENAS].loc[mask2, :] = Reemplazar_columna_en_funcion_de_otra(
    df=consultas_mod[CONSULTA_CADENAS].loc[mask2, :].copy(),
    nom_columna_a_reemplazar=SEGMENTO_AGRUP,
    nom_columna_de_referencia=SEGMENTO_TRANS,
    mapeo=DICT_SEG_AGRUP_NO_DS,
)

# Los valores restantes que quedan sin asignar en Segmento Agrupado.
# Eliminar la columna adicional temporal.
consultas_mod[CONSULTA_CADENAS].drop(CONCATENADA, axis=1, inplace=True)

