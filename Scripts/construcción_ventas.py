# from fusion_consolidado import consulta_agrupada
import pandas as pd
from unidecode import unidecode
from lectura_consultas_ventas import ventas_por_tipo
from Exclusive_CxS_functions import Filtrar_DataFrames_ventas
from Trasformation_functions import concatenar_columnas
from General_Functions import Group_by_and_sum
from config_constans import (
    config,
    CONCATENADA,
    SECTOR,
    FORMATO,
    VENTA_CADENAS,
    VENTA_COMP,
    CANAL_DIST,
    COLS_NECESARIAS_VENTAS_M,
    COLS_NECESARIAS_VENTAS_SA,
    VENTAS,
    TIPO_VENTA_COMP,
    TIPO_VENTA_CADENAS,
    COL_GRANDES_CADENAS,
    SUBCANAL_SEGMENTO_2,
    DEVS_MALAS,
    DEVS_SA,
)

# Remover los acentos de las columnas para manejar el dataframe más facilmente.
for key in ventas_por_tipo.keys():
    for i in range(2):
        nuevos_nombres = [
            unidecode(columna) for columna in ventas_por_tipo[key][i].columns
        ]
        ventas_por_tipo[key][i].columns = nuevos_nombres

venta_cadena_sin_asignar = ventas_por_tipo[VENTA_CADENAS][1]
venta_cadena_malas = ventas_por_tipo[VENTA_CADENAS][0]
venta_comp_sin_asignar = ventas_por_tipo[VENTA_COMP][1]
venta_comp_malas = ventas_por_tipo[VENTA_COMP][0]

# Tomamos de la configuración las columnas necesarias para "compañias"
cols_filtrar_compañia_m = config[VENTAS][TIPO_VENTA_COMP][COLS_NECESARIAS_VENTAS_M]

cols_filtrar_cadenas_m = config[VENTAS][TIPO_VENTA_CADENAS][COLS_NECESARIAS_VENTAS_M]

cols_filtrar_compañia_sa = config[VENTAS][TIPO_VENTA_COMP][COLS_NECESARIAS_VENTAS_SA]

cols_filtrar_cadenas_sa = config[VENTAS][TIPO_VENTA_CADENAS][COLS_NECESARIAS_VENTAS_SA]

# Filtramos los dataframe de compañia. Malas y "Sin asignar "
venta_comp_malas_filtrada = Filtrar_DataFrames_ventas(
    df=venta_comp_malas,
    col_a_filtrar=CANAL_DIST,
    val_a_filtrar=COL_GRANDES_CADENAS,
    cols_necesarias=cols_filtrar_compañia_m,
)

venta_comp_sin_asignar_filtrada = Filtrar_DataFrames_ventas(
    df=venta_comp_sin_asignar,
    col_a_filtrar=CANAL_DIST,
    val_a_filtrar=COL_GRANDES_CADENAS,
    cols_necesarias=cols_filtrar_compañia_sa,
)

# Tomamos los dataframes de cadenas. Malas y "Sin asignar"
venta_cadena_sin_asignar_filtrada = venta_cadena_sin_asignar[cols_filtrar_cadenas_sa]
venta_cadena_malas_filtrada = venta_cadena_malas[cols_filtrar_cadenas_m]

# Agregar la columna "concatenada" para cadenas(Concatenar:  "Sector" y "Formato")
# Agregar la columna "concatenada" para compañia(Concatenar: "Sector y "Sub Canal / Segmento 2")

# Creamos las cpias para evitar advertencias con el mal manejo de datos.
venta_cadena_malas_filtrada = venta_cadena_malas_filtrada.copy()
venta_cadena_sin_asignar_filtrada = venta_cadena_sin_asignar_filtrada.copy()
venta_comp_malas_filtrada = venta_comp_malas_filtrada.copy()
venta_comp_sin_asignar_filtrada = venta_comp_sin_asignar_filtrada.copy()

venta_cadena_malas_filtrada = concatenar_columnas(
    venta_cadena_malas_filtrada, SECTOR, FORMATO, CONCATENADA
)
venta_cadena_sin_asignar_filtrada = concatenar_columnas(
    venta_cadena_sin_asignar_filtrada, SECTOR, FORMATO, CONCATENADA
)
venta_comp_malas_filtrada = concatenar_columnas(
    venta_comp_malas_filtrada, SECTOR, SUBCANAL_SEGMENTO_2, CONCATENADA
)
venta_comp_sin_asignar_filtrada = concatenar_columnas(
    venta_comp_sin_asignar_filtrada, SECTOR, SUBCANAL_SEGMENTO_2, CONCATENADA
)

# Unimos las devoluciones malas.
concat_dev_malas = pd.concat(
    [
        venta_cadena_malas_filtrada[[CONCATENADA, DEVS_MALAS]],
        venta_comp_malas_filtrada[[CONCATENADA, DEVS_MALAS]],
    ],
    axis=0,
)
# Unimos las devoluciones Sin Asignar.
concat_dev_sin_asignar = pd.concat(
    [
        venta_cadena_sin_asignar_filtrada[[CONCATENADA, DEVS_SA]],
        venta_comp_sin_asignar_filtrada[[CONCATENADA, DEVS_SA]],
    ],
    axis=0,
)

concat_dev_malas[DEVS_MALAS] = concat_dev_malas[DEVS_MALAS].astype(float)
concat_dev_sin_asignar[DEVS_SA] = concat_dev_sin_asignar[DEVS_SA].astype(float)

# Agrupar las devoluciones por claves en "concatenada"
concat_dev_malas_gb = Group_by_and_sum(concat_dev_malas, CONCATENADA, DEVS_MALAS)

concat_dev_sin_asignar_gb = Group_by_and_sum(
    concat_dev_sin_asignar, CONCATENADA, DEVS_SA
)

# Convertir en diccionarios.
dict_dev_sa = dict(
    zip(concat_dev_sin_asignar_gb[CONCATENADA], concat_dev_sin_asignar_gb[DEVS_SA])
)

dict_dev_m = dict(
    zip(concat_dev_malas_gb[CONCATENADA], concat_dev_malas_gb[DEVS_MALAS])
)
