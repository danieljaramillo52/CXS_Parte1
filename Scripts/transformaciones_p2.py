# Importaciones functions y variables
from transformaciones_p1 import consultas_mod
from Exclusive_CxS_functions import Reemplazos_multiples_columnas
from Trasformation_functions import Reemplazar_columna_en_funcion_de_otra

# Importar constantes necesarias.
from config_constans import (
    CANAL,
    SUBCANAL,
    CANAL_TRANS,
    DISTRIBUIDORES,
    SUBCANAL_TRANS,
    SEGMENTO_TRANS,
    SEGMENTO_AGRUP,
    FORMATO,
    FORMATO_NIF,
    FIL_SIN_ASIGNAR,
    CLIENTE,
    CONSULTA_CADENAS,
    CONSULTA_DS,
    CONSULTA_NO_DS,
    COLS_CNAL_SUBCNAL_SEG,
    DICT_REMP_CNAL_SUBC_SEG_DS,
    TRADICIONAL,
    RAMO_CLAVE,
    AGENTE_COMERCIAL,
)

from config_dicts import (
    DICT_REMPLAZAR_FORMATO_CADENAS,
    DICT_SEG_AGRUP_NO_DS,
    DICT_REMP_CLASIFICACION_DS,
    
)
# Trasformaciones de este Script.
# 1. Modificación en consulta_cadenas (Col => Formato) según algunos N.I.F especificos.
# (Lista corta columna driver cadenas col D Y E.)

# 2. Modificación en consulta_cadenas (Cols: Formato y Formato N.I.F  si Formato N.I.F ==  # ) (Lista corta columna driver cadenas col G, H, Y, J, K, L).

# Modificaciones en la consulta DS.
# 3. Remplazar todos los valores de la columna Formato como "Sin asignar".
# 4. Agregar columnas Canal y Subcanal a todos los dataframes.
# 5. Modificar Col Segmento agrupado en consulta_no_ds.

# 1
consultas_mod[CONSULTA_CADENAS] = Reemplazar_columna_en_funcion_de_otra(
    df=consultas_mod[CONSULTA_CADENAS],
    nom_columna_a_reemplazar=FORMATO,
    nom_columna_de_referencia=FORMATO_NIF,
    mapeo=DICT_REMPLAZAR_FORMATO_CADENAS,
)

# Modifcaciones para la consulta ds.
#Estandarizar los Agentes comerciales. ( drivers y consulta con c minuscula.)
consultas_mod[CONSULTA_DS][SEGMENTO_TRANS]=consultas_mod[CONSULTA_DS][SEGMENTO_TRANS].replace("Agente Comercial", AGENTE_COMERCIAL)

# 1.) A todos los clientes primero se les asigna el formato "Sin asignar"
consultas_mod[CONSULTA_DS][FORMATO] = FIL_SIN_ASIGNAR
consultas_mod[CONSULTA_NO_DS][FORMATO].astype(str)
consultas_mod[CONSULTA_NO_DS][FORMATO] = FIL_SIN_ASIGNAR

# 2.) A todos los Canales_Trans, Subcanales_Trans, Segmentos_Trans que sean "Sin asignar"
# Poderles asignar algo. Cambiado a voluntad.
# USO DEL DICT FIL_SIN_ASIGNAR.
consultas_mod[CONSULTA_DS] = Reemplazos_multiples_columnas(
    df=consultas_mod[CONSULTA_DS],
    columnas_a_verificar=COLS_CNAL_SUBCNAL_SEG,
    reemplazos=DICT_REMP_CNAL_SUBC_SEG_DS,
    nom_condicion=FIL_SIN_ASIGNAR,
    col_clave=RAMO_CLAVE,
)

# Agregar columnas canal y subcanal a todos los dataframes.
columns_to_add = [CANAL, SUBCANAL]
for columna in columns_to_add:
    for nombre_dataframe, dataframe in consultas_mod.items():
        dataframe[columna] = dataframe[f"{columna}_Trans"]
        print(f"Columna '{columna}' agregada a '{nombre_dataframe}' DataFrame.")

# Asignar a la columna canal de "DS" el formato "Tradicional".
consultas_mod[CONSULTA_DS][CANAL] = "Tradicional"

# Remplazar Subcanal. Según driver Ds. (con el uso de la columna clasificacion)
consultas_mod[CONSULTA_DS] = Reemplazar_columna_en_funcion_de_otra(
    df=consultas_mod[CONSULTA_DS],
    nom_columna_a_reemplazar=SUBCANAL,
    nom_columna_de_referencia=FORMATO_NIF,
    mapeo=DICT_REMP_CLASIFICACION_DS,
)

# Remplazar Can_Tras , Sub_Ctrans , Segmen_Trans. SI SUBCANAL == Distribuidores.
# Canal_Trans pasa a ser Tradicional , SubCT y SegT pasan a ser Distribuidores,
# Seg Agrup Tambien.
consultas_mod[CONSULTA_DS].loc[
    consultas_mod[CONSULTA_DS][SUBCANAL] == DISTRIBUIDORES, CANAL_TRANS
] = TRADICIONAL
consultas_mod[CONSULTA_DS].loc[
    consultas_mod[CONSULTA_DS][SUBCANAL] == DISTRIBUIDORES, SUBCANAL_TRANS
] = DISTRIBUIDORES
consultas_mod[CONSULTA_DS].loc[
    consultas_mod[CONSULTA_DS][SUBCANAL] == DISTRIBUIDORES, SEGMENTO_AGRUP
] = DISTRIBUIDORES
consultas_mod[CONSULTA_DS].loc[
    consultas_mod[CONSULTA_DS][SUBCANAL] == DISTRIBUIDORES, SEGMENTO_TRANS
] = DISTRIBUIDORES

# Remplazar el Segmento Agrupado. igual que como hicimos en la consulta
# no_ds == sin clientes
#mask = consultas_mod[CONSULTA_DS][SUBCANAL_TRANS] == AGENTE_COMERCIAL

# Aplicar la función solo en las filas seleccionadas por la máscara
consultas_mod[CONSULTA_DS] = Reemplazar_columna_en_funcion_de_otra(
    df=consultas_mod[CONSULTA_DS],
    nom_columna_a_reemplazar=SEGMENTO_AGRUP,
    nom_columna_de_referencia=SEGMENTO_TRANS,
    mapeo=DICT_SEG_AGRUP_NO_DS,
)

#consultas_mod[CONSULTA_DS][[SUBCANAL,SEGMENTO_TRANS,SEGMENTO_AGRUP]].to_excel("DS_reemplazos_listos.xlsx",index=False)
consultas_para_consolidar = consultas_mod.copy()
