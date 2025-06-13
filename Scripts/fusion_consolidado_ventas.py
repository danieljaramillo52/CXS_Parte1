import pandas as pd
from unidecode import unidecode
from General_Functions import Group_by_and_sum,Procesar_configuracion
from Trasformation_functions import (
    Reemplazar_columna_en_funcion_de_otra,
    concatenar_columnas,
)
from datetime import datetime
from fusion_consolidado import consulta_doble_numeral_agrup, consulta_secos_agrup
from construcción_ventas import dict_dev_sa, dict_dev_m

config = Procesar_configuracion(nom_archivo_configuracion="config.yml")

from config_constans import (
    SUBCANAL,
    SECTOR,
    FORMATO,
    DEVS_MALAS,
    DEVS_SA,
    CONCATENADA_CAD,
    CONCATENADA_COMP,
    COLS_NUMERICAS,
    CENTRO_COS,
    FIL_SIN_ASIGNAR,
    VENTA_NETAS_CN,
    CONCATENADA_GB_VN,
    DEVS_PARAMETRO,
    DEVS_COL_FINAL_MA,
    DEVS_COL_FINAL_SA, 
    DEVS_MALAS_COL_FINAL,
    VENTA_NETAS_GRUPO,
    VENTA_EFECTIVA_FINAL,
    config,
)

# Creamos una copia de la consulta original y le agrgaremos las columnas concatenadas,
# Para traer toda la información de las devoluciones malas y sin asignar, tanto de compañia como de cadenas.

consulta_doble_numeral_agrup_filtrada = (
    consulta_doble_numeral_agrup[
        [SECTOR, SUBCANAL, FORMATO, CENTRO_COS, VENTA_NETAS_GRUPO]
    ]
).copy()

argumentos_concatenar = [
    (SECTOR, SUBCANAL, CONCATENADA_COMP),
    (SECTOR, FORMATO, CONCATENADA_CAD),
]

# Creamos las columnas concatenadas de referencia para compañias y cadenas.
for args in argumentos_concatenar:
    consulta_doble_numeral_concat = concatenar_columnas(
        consulta_doble_numeral_agrup, *args
    )

# Crear las columnas para las devoluciones.
consulta_doble_numeral_concat[DEVS_SA] = 0
consulta_doble_numeral_concat[DEVS_MALAS] = 0

# Lista de argumentos y mapeos
argumentos_devoluciones = [
    (DEVS_SA, CONCATENADA_COMP, dict_dev_sa),
    (DEVS_MALAS, CONCATENADA_COMP, dict_dev_m),
    (DEVS_SA, CONCATENADA_CAD, dict_dev_sa),
    (DEVS_MALAS, CONCATENADA_CAD, dict_dev_m),
]

insumo_devoluciones = consulta_doble_numeral_concat.copy()
for args in argumentos_devoluciones:
    insumo_devoluciones = Reemplazar_columna_en_funcion_de_otra(
        insumo_devoluciones, *args
    )

# Tomar el insumo_de devoluciones como insumo para agrupación final.
insumo_dev = insumo_devoluciones[
    [
        CENTRO_COS,
        FORMATO,
        CONCATENADA_CAD,
        CONCATENADA_COMP,
        VENTA_NETAS_GRUPO,
        DEVS_MALAS,
        DEVS_SA,
    ]
]
insumo_dev_filtrado = insumo_dev.copy()

filtrado_by_comp = insumo_dev_filtrado[
    insumo_dev_filtrado[FORMATO].isin([FIL_SIN_ASIGNAR])
].drop(columns=[CONCATENADA_CAD])

filtrado_by_cad = insumo_dev_filtrado[
    ~insumo_dev_filtrado[FORMATO].isin([FIL_SIN_ASIGNAR])
].drop(columns=[CONCATENADA_COMP])

# Renombrar concatenadas para unirlas.
filtrado_by_cad.rename(columns={CONCATENADA_CAD: CONCATENADA_GB_VN}, inplace=True)
filtrado_by_comp.rename(columns={CONCATENADA_COMP: CONCATENADA_GB_VN}, inplace=True)

concat_cad_comp = pd.concat([filtrado_by_cad, filtrado_by_comp], axis=0)

# Agrupar por la columna CONCATENADA_GB_VN
# (Concatenada Group_by_ventas_netas.)

group_by_vn = Group_by_and_sum(
    df=concat_cad_comp, group_col=CONCATENADA_GB_VN, sum_col=VENTA_NETAS_GRUPO
)
# Crear diccionarios para usar valores.
dict_vn_agrup = group_by_vn.set_index(CONCATENADA_GB_VN)[VENTA_NETAS_GRUPO].to_dict()


# Itera a través de las filas del DataFrame
for cada_tipo_dev in [DEVS_MALAS, DEVS_SA]:
    for index, row in concat_cad_comp.iterrows():
        clave = row[CONCATENADA_GB_VN]  # Obtiene la clave de la columna1
        venta_neta = row[VENTA_NETAS_GRUPO]  # Obtiene el valor de la columna2
        dev_mala = row[cada_tipo_dev]  # Obtiene el valor de la columna3

        if clave in dict_vn_agrup:
            valor = dict_vn_agrup[clave]

            if valor==0:
                pass
            else: 
                resultado_multiplicacion = venta_neta * dev_mala
                resultado_division = resultado_multiplicacion / valor

            # Guarda el resultado en col DEVS_MALAS
                concat_cad_comp.at[index, cada_tipo_dev] = resultado_division

# Agregar columnas adicionales.
consulta_doble_numeral_agrup.loc[:,DEVS_MALAS] = 0.0
consulta_doble_numeral_agrup.loc[:,DEVS_SA] = 0.0

consulta_doble_numeral_agrup.loc[:, DEVS_MALAS] = concat_cad_comp[DEVS_MALAS].values
consulta_doble_numeral_agrup.loc[:, DEVS_SA] = concat_cad_comp[DEVS_SA].values

# Agregar la columna de Devoluciones a la consulta de los secos.
consulta_secos_agrup[DEVS_MALAS_COL_FINAL] = 0

# Agregar la columna de devoluciones finales a la consulta doble_numeral.
# Establecer el valor total de la distribución.
Total_devs_malas = consulta_doble_numeral_agrup[DEVS_MALAS].sum()
Total_devs_SA = consulta_doble_numeral_agrup[DEVS_SA].sum()
Total_devs = Total_devs_malas + Total_devs_SA

# Agregar la columna que nos interesa de devoluciones malas. ( Final )
consulta_doble_numeral_agrup[DEVS_COL_FINAL_MA] = (
    (consulta_doble_numeral_agrup[DEVS_MALAS] / Total_devs
) * DEVS_PARAMETRO)

consulta_doble_numeral_agrup[DEVS_COL_FINAL_SA] = (
    (consulta_doble_numeral_agrup[DEVS_SA] / Total_devs
) * DEVS_PARAMETRO)

consulta_doble_numeral_agrup[DEVS_MALAS_COL_FINAL] = consulta_doble_numeral_agrup[DEVS_COL_FINAL_SA] + consulta_doble_numeral_agrup[DEVS_COL_FINAL_MA]

# Ventas_Efectivas Final. Agregar.
consulta_secos_agrup[VENTA_EFECTIVA_FINAL] = 0
consulta_doble_numeral_agrup[VENTA_EFECTIVA_FINAL] = (
    consulta_doble_numeral_agrup[DEVS_MALAS_COL_FINAL]
    + consulta_doble_numeral_agrup[VENTA_NETAS_GRUPO]
)


# Eliminar columnas adicionales de la consulta agrupada doble numeral
consulta_doble_numeral_agrup_copy = consulta_doble_numeral_agrup.copy()
consulta_doble_numeral_agrup_copy.drop(
    columns={CONCATENADA_COMP, CONCATENADA_CAD, DEVS_SA, DEVS_MALAS,DEVS_COL_FINAL_SA,DEVS_COL_FINAL_MA}, inplace=True
)

consulta_final = pd.concat(
    [consulta_secos_agrup, consulta_doble_numeral_agrup_copy], axis=0
)

A=input("¿Esta trabajando con Real ó Presupuesto?: (R/P): ")
if A=="R":
    consulta_final["Tipo"] = "Real"
elif A=="P": 
    consulta_final["Tipo"] = "Presupuesto"
    
consulta_final_mod = consulta_final.astype(str)

# Ruta de exportación.
Ruta = config["path"]["Resultados"]

consulta_final_mod[COLS_NUMERICAS] = (
    consulta_final_mod[COLS_NUMERICAS].astype(float).round(2)
)


