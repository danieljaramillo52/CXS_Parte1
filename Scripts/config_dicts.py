# Aqui vamos a leer y procesar todos los drivers necesarios para el proceso.
from config_constans import (
    RAMO_CLAVE,
    CANAL_TRANS,
    SUBCANAL_TRANS,
    SEGMENTO_TRANS,
    SEGMENTO_AGRUP,
    UNION,
    NIF,
    FORMAT,
    FORMATO,
    FORMATO_NIF,
    CLASIFICACION,
    CLIENTE_CET,
)
from General_Functions import Crear_diccionario_con_listas
from lectura_drivers import driver_no_ds, driver_cadenas, driver_ds

# Creamos el diccionario DICT_REMP_CNAL_SUBCNAL_SEG_NO_DS
df_para_dict1 = driver_no_ds[[RAMO_CLAVE, CANAL_TRANS, SUBCANAL_TRANS, SEGMENTO_TRANS]]
df_para_dict1.dropna(subset=list(df_para_dict1.columns))
DICT_REMP_CNAL_SUBCNAL_SEG_NO_DS = Crear_diccionario_con_listas(
    dataframe=df_para_dict1, col_clave=RAMO_CLAVE
)

# Creamos el diccionario DICT_SEG_AGRUP_NO_DS
DICT_SEG_AGRUP_NO_DS = driver_no_ds.set_index(f'{SEGMENTO_TRANS}{".1"}')[
    SEGMENTO_AGRUP
].to_dict()
# Creamos el diccionario DICT_SEG_AGRUP_CADENAS
DICT_SEG_AGRUP_CADENAS = driver_cadenas.set_index(UNION)[SEGMENTO_TRANS].to_dict()

# Creamos el diccionario DICT_REMPLAZAR_FORMATO_CADENAS
DICT_REMPLAZAR_FORMATO_CADENAS = driver_cadenas.set_index(NIF)[FORMAT].to_dict()

# NK hace referencia a number key osea los registros cuyo Formato N.I.F == (#)
# DICT_REMP_CNAL_SUBC_SEG_FOR_FORNIF_CADENAS_NK
# Este diccionario hace referencia a cuando debemos asignar información a las columnas,
# donde el Formato es == "Sin asignar", y donde Formato_N.I.F es IGUAL A #
# (Aqui se puede asignar, dependiendo del ramo, cualquier (CanalT,SubCT,SegT)

# LOS ELEMENTOS SON:
# RAMO_CLAVE:[ "Canal_Trans", "Sub_Canal_Trans", "Seg_Trans", "Formato", "Formato_N.I.F" ]
df_para_dict5 = driver_cadenas[
    [
        RAMO_CLAVE,
        CANAL_TRANS,
        SUBCANAL_TRANS,
        f'{SEGMENTO_TRANS}{".1"}',
        FORMATO_NIF,
    ]
]
df_para_dict5.dropna(subset=list(df_para_dict5.columns))
DICT_REMP_CNAL_SUBC_SEG_FOR_FORNIF_CADENAS_NK = Crear_diccionario_con_listas(
    dataframe=df_para_dict5, col_clave=RAMO_CLAVE
)

# Creamos el diccionario DICT_REMP_CNAL_SUBC_SEG_MAL_CREADOS
# Este diccionario sirve para identificar los clientes Mal Creados, pero que tienen
# Un formato asignado por ejemplo OXXO , y por lo tanto tienen un valor fino de Canal_Transformado, Subcanal_Trasformad y Segmento Trasformado para remplazar  sus 'sin asignar'
df_para_dict8 = driver_cadenas[
    [
        f'{FORMATO}',
        f'{CANAL_TRANS}{".1"}',
        f'{SUBCANAL_TRANS}{".1"}',
        f'{SEGMENTO_TRANS}{".2"}',
        f'{FORMATO_NIF}{".1"}',
    ]
]
df_para_dict8.dropna(subset=list(df_para_dict8.columns))
DICT_CLIENTE_FORMATO_FALTANTES = Crear_diccionario_con_listas(
    dataframe=df_para_dict8, col_clave=f'{FORMATO}'
)


df_para_dict10 = driver_cadenas[
    [
        f'{FORMATO}{".1"}',
        f'{CANAL_TRANS}{".2"}',
        f'{SUBCANAL_TRANS}{".2"}',
        f'{SEGMENTO_TRANS}{".3"}',
    ]
]
DICT_REMP_CNAL_SUBC_SEG_MAL_CREADOS = Crear_diccionario_con_listas(
    dataframe=df_para_dict10, col_clave=f'{FORMATO}{".1"}')
# DICT_CLIENTE_SIN_ASIGNAR_FALTANTES (#Falta un grupo de clientes en cadenas que no está asignado a nada.
# CT,SUBCT,SEGT Y F == "Sin asignar" Pero F.I.F != "Sin asignar" (Osea está definido)
df_para_dict9 = driver_cadenas[
    [
        f'{RAMO_CLAVE}{".1"}',
        f'{CANAL_TRANS}{".3"}',
        f'{SUBCANAL_TRANS}{".3"}',
        f'{SEGMENTO_TRANS}{".4"}',
    ]
]
DICT_CLIENTE_SIN_ASIGNAR_FALTANTES = Crear_diccionario_con_listas(
    dataframe=df_para_dict9, col_clave=f'{RAMO_CLAVE}{".1"}'
)

# Creamos el diccionario DICT_SEG_AGRUP_NO_DS
DICT_SEG_AGRUP_NO_DS = driver_no_ds.set_index(f'{SEGMENTO_TRANS}{".1"}')[
    SEGMENTO_AGRUP
].to_dict()
# Creamos el diccionario DICT_SEG_AGRUP_CADENAS
DICT_SEG_AGRUP_CADENAS = driver_cadenas.set_index(UNION)[SEGMENTO_TRANS].to_dict()

# Creamos el diccionario DICT_REMPLAZAR_FORMATO_CADENAS
DICT_REMPLAZAR_FORMATO_CADENAS = driver_cadenas.set_index(NIF)[FORMAT].to_dict()

DICT_REMP_CLIENTE_DS = driver_ds.set_index(NIF)[CLIENTE_CET].to_dict()

DICT_REMP_CLASIFICACION_DS = driver_ds.set_index(NIF)[CLASIFICACION].to_dict()
