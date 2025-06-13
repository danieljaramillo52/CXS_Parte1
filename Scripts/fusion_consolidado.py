from transformaciones_p2 import consultas_para_consolidar
from Trasformation_functions import (
    modificaciones_elegidas_dataframe,
    Reemplazar_columna_en_funcion_de_otra,
)
from pandas import concat, eval
from config_constans import (
    FORMATO_NIF,
    FILLNA,
    ASTYPE,
    CENTRO_COS,
    COLS_FOR_GROUPBY_DN,
    COLS_NO_NUMERICAS,
    COLS_FOR_GROUPBY_SECOS,
    COLS_NECESARIAS,
    COLS_FOR_SUM,
    VENTA_EFECTIVA,
    VENTA_NETAS_CN,
    VENTA_NETAS_GRUPO,
    VENTA_EFECTIVA_GRUPO,
    CLIENTE,
    OFICINA_VENTAS,
    OFICINA_VENTAS_AGRUP,
    FIL_DOBLE_NUMERAL,
    FIL_SIN_ASIGNAR,
    DESCUENTOS,
    DESCUENTOS_NG,
    DESCUENTOS_CN,
    GASTO_PROM_COMER,
    DEPURACION_DCTOS,
    DESCUENTOS_GRUPO,
    DICT_REGIONES,
)
from config_dicts import DICT_REMP_CLIENTE_DS

# De cada diccionario extraemos cada consulta con las columnas necesarias.
consultas_para_consolidar_mod = consultas_para_consolidar.copy()

for key in consultas_para_consolidar_mod.keys():
    # Tomar las columnas necesarias de cada df
    consultas_para_consolidar_mod[key] = consultas_para_consolidar[key][COLS_NECESARIAS]
    # Cambiar el tipo de dato de las columnas necesarias.
    # Trasformar los datos a reales para hacer la agrupación.

operaciones = [FILLNA, ASTYPE]
for i in range(len(operaciones)):
    for key in consultas_para_consolidar_mod.keys():
        # Modificar el tipo de dato de las columnas COLS_FOR_SUM para cada 'key' en consultas_para_consolidar_mod
        consultas_para_consolidar_mod = modificaciones_elegidas_dataframe(
            consultas_para_consolidar_mod, key, COLS_FOR_SUM, operaciones[i]
        )

# Concatenar todas las fuentes de información.
consulta_consolidado = concat(consultas_para_consolidar_mod.values(), axis=0)

# LLenar espacios vacios del dataframe.
consulta_consolidado[COLS_NO_NUMERICAS] = consulta_consolidado[
    COLS_NO_NUMERICAS
].fillna(FIL_SIN_ASIGNAR)

# Vamos a dividir la consulta en 2 partes la parte que contiene los secos. ( Y la parte que contiene la información (Ventas,Descuentos, Gasto_prom C...) == Secos #/#)

consulta_doble_numeral = consulta_consolidado[
    consulta_consolidado[CENTRO_COS].isin([FIL_DOBLE_NUMERAL])
]


consulta_secos = consulta_consolidado[
    ~consulta_consolidado[CENTRO_COS].isin([FIL_DOBLE_NUMERAL])
]

# Hacer el groupby para imitar la tabla dinámica.
# Doble numeral.
consulta_doble_numeral_agrup = consulta_doble_numeral.groupby(
    COLS_FOR_GROUPBY_DN, as_index=False,
)[COLS_FOR_SUM].sum()

# Tabla secos.
consulta_secos_agrup = consulta_secos.groupby(
    COLS_FOR_GROUPBY_SECOS, as_index=False, dropna=False
)[COLS_FOR_SUM].sum()

# Definir columnas previas:
consulta_doble_numeral_agrup.copy()

consulta_doble_numeral_agrup.loc[
    :, DEPURACION_DCTOS
] = consulta_doble_numeral_agrup.loc[:, DESCUENTOS_NG]

consulta_doble_numeral_agrup.loc[
    :, DESCUENTOS_GRUPO
] = consulta_doble_numeral_agrup.loc[:, DESCUENTOS]

consulta_doble_numeral_agrup.loc[
    :, VENTA_EFECTIVA_GRUPO
] = consulta_doble_numeral_agrup.loc[:, VENTA_EFECTIVA]


# Crear un diccionario de operaciones y columnas
operaciones = {
    DESCUENTOS_CN: "consulta_doble_numeral_agrup[DESCUENTOS_GRUPO] - consulta_doble_numeral_agrup[GASTO_PROM_COMER]",
    VENTA_NETAS_CN: "consulta_doble_numeral_agrup[VENTA_EFECTIVA_GRUPO] - (consulta_doble_numeral_agrup[DESCUENTOS_GRUPO] - consulta_doble_numeral_agrup[GASTO_PROM_COMER])",
    VENTA_NETAS_GRUPO: "consulta_doble_numeral_agrup[VENTA_EFECTIVA_GRUPO] - (consulta_doble_numeral_agrup[DESCUENTOS_GRUPO] - consulta_doble_numeral_agrup[GASTO_PROM_COMER]) - consulta_doble_numeral_agrup[GASTO_PROM_COMER]",
}

# Aplicar el diccionario de operaciones a 'consulta_doble_numeral' usando eval
for columna, formula in operaciones.items():
    consulta_doble_numeral_agrup.loc[:, columna] = eval(formula)

# Lista de columnas que se inicializarán en 0 en 'consulta_secos'
columnas_inicializar_en_ceros = [
    DEPURACION_DCTOS,
    DESCUENTOS_GRUPO,
    VENTA_EFECTIVA_GRUPO,
    DESCUENTOS_CN,
    VENTA_NETAS_CN,
    VENTA_NETAS_GRUPO,
]

# Aplicar inicialización en 0 a 'consulta_secos'
for columna in columnas_inicializar_en_ceros:
    consulta_secos_agrup = consulta_secos_agrup.copy()
    consulta_secos_agrup.loc[:, columna] = 0

# Crear col "Cliente" y "Oficina_ventas_Agrup"
consulta_doble_numeral_agrup[CLIENTE] = FIL_SIN_ASIGNAR
consulta_secos_agrup[CLIENTE] = FIL_SIN_ASIGNAR
consulta_doble_numeral_agrup[OFICINA_VENTAS_AGRUP] = FIL_SIN_ASIGNAR
consulta_secos_agrup[OFICINA_VENTAS_AGRUP] = FIL_SIN_ASIGNAR
# Agregar la información de clientes y/o regionales.
# Clientes
consulta_doble_numeral_agrup = Reemplazar_columna_en_funcion_de_otra(
    df=consulta_doble_numeral_agrup,
    nom_columna_a_reemplazar=CLIENTE,
    nom_columna_de_referencia=FORMATO_NIF,
    mapeo=DICT_REMP_CLIENTE_DS,
)

consulta_secos_agrup = Reemplazar_columna_en_funcion_de_otra(
    df=consulta_secos_agrup,
    nom_columna_a_reemplazar=CLIENTE,
    nom_columna_de_referencia=FORMATO_NIF,
    mapeo=DICT_REMP_CLIENTE_DS,
)

# Regiones
consulta_doble_numeral_agrup = Reemplazar_columna_en_funcion_de_otra(
    df=consulta_doble_numeral_agrup,
    nom_columna_a_reemplazar=OFICINA_VENTAS_AGRUP,
    nom_columna_de_referencia=OFICINA_VENTAS,
    mapeo=DICT_REGIONES,
)

consulta_secos_agrup = Reemplazar_columna_en_funcion_de_otra(
    df=consulta_secos_agrup,
    nom_columna_a_reemplazar=OFICINA_VENTAS_AGRUP,
    nom_columna_de_referencia=OFICINA_VENTAS,
    mapeo=DICT_REGIONES,
)
