# Proceso de exportar resultados a una base de datos.

from itertools import combinations
import xlsxwriter
from loguru import logger
import General_Functions as gf
import Trasformation_functions as tf
from Exclusive_CxS_functions import generar_agrupaciones, calcular_diferencias_gastos
from fusion_consolidado_ventas import consulta_final_mod, config

# Cambiar el nombre de la columna formato N.I.F
consulta_final_mod = consulta_final_mod.rename(columns=config["columnas_db_dict"])

list_tablas_cxs = ["td_cxs_ppto_estatico", "td_cxs_real"]
df_verf_vtas_gn = gf.Lectura_insumos_excel(
    path=config["path"]["Validacion"],
    nom_insumo=config["Consultas"]["ventas_verificar"]["file_name"],
    nom_Hoja=config["Consultas"]["ventas_verificar"]["sheet"][0],
    cols=config["Consultas"]["ventas_verificar"]["cols"][0],
)
df_verf_vtas_gn = gf.Eliminar_acentos(df_verf_vtas_gn)

df_verf_vtas_cad = gf.Lectura_insumos_excel(
    path=config["path"]["Validacion"],
    nom_insumo=config["Consultas"]["ventas_verificar"]["file_name"],
    nom_Hoja=config["Consultas"]["ventas_verificar"]["sheet"][1],
    cols=config["Consultas"]["ventas_verificar"]["cols"][1],
)

df_gastos_cn = gf.Lectura_insumos_excel(
    path=config["path"]["Validacion"],
    nom_insumo=config["Consultas"]["gastos_verificar"]["file_name"],
    nom_Hoja=config["Consultas"]["gastos_verificar"]["sheet"],
    cols=config["Consultas"]["gastos_verificar"]["cols"][0],
)

df_verf_vtas_gn_ren = tf.Renombrar_columnas_con_diccionario(
    base=df_verf_vtas_gn,
    cols_to_rename=config["Consultas"]["ventas_verificar"]["renombrar_columnas_GN"],
)

df_verf_vtas_cad_ren = tf.Renombrar_columnas_con_diccionario(
    base=df_verf_vtas_cad,
    cols_to_rename=config["Consultas"]["ventas_verificar"]["renombrar_columnas_FOR"],
)

df_verf_vtas_gn_ren = tf.eliminar_fila_por_indice(df_verf_vtas_gn_ren, 0)
df_verf_vtas_cad_ren = tf.eliminar_fila_por_indice(df_verf_vtas_cad_ren, 0)


# Agrupación general de vtas por todas la columnas:
campos = config["Consultas"]["ventas_verificar"]["campos"]

group_columns = campos[:5]  # columnas para agrupar
sum_columns = campos[5:]  # columnas a sumar

consulta_final_mod_fil = tf.filtrar_por_valores(
    df=consulta_final_mod, columna="centro_costo", valores=["#/#"]
)
consulta_final_mod_fil_gc = tf.filtrar_por_valores(
    df=consulta_final_mod_fil, columna="canal_trans", valores=["Grandes Cadenas"]
)

consulta_final_mod_fil = consulta_final_mod[campos]
df_verf_vtas_gn_ren = df_verf_vtas_gn_ren[campos[1:]]


# Normaliza tipos de datos primero
df_verf_vtas_gn_ren = tf.Cambiar_tipo_dato_multiples_columnas_pd(
    base=df_verf_vtas_gn_ren, list_columns=campos[5:], type_data=float
)
df_verf_vtas_cad_ren = tf.Cambiar_tipo_dato_multiples_columnas_pd(
    base=df_verf_vtas_cad_ren, list_columns=campos[5:], type_data=float
)

# Generar ambos sets
agrupaciones_individuales, agrupaciones_combinadas = generar_agrupaciones(
    consulta_final_mod_fil,
    consulta_final_mod_fil_gc,
    group_columns,
    sum_columns,
    incluir_formato_sector=True,
)

agrupaciones_individuales1, agrupaciones_combinadas1 = generar_agrupaciones(
    df_verf_vtas_gn_ren,
    df_verf_vtas_cad_ren,
    group_columns,
    sum_columns,
    incluir_formato_sector=True,
)

# Comparar resultados
resultado_indiv_vs_indiv1 = tf.comparar_diccionarios_df(
    dict1=agrupaciones_individuales,
    dict2=agrupaciones_individuales1,
    sum_columns=["ventas_netas_cn", "descuentos_cn"],
)

resultado_combi_vs_combi1 = tf.comparar_diccionarios_df(
    dict1=agrupaciones_combinadas,
    dict2=agrupaciones_combinadas1,
    sum_columns=["ventas_netas_cn", "descuentos_cn"],
)

# Base de gastos comparaciones.
df_gastos_cn = tf.Renombrar_columnas_con_diccionario(
    base=df_gastos_cn,
    cols_to_rename=config["Consultas"]["gastos_verificar"]["renombrar_columnas_GN"],
)
consulta_final_mod_elim = tf.eliminar_primeros_n_caracteres(
    df=consulta_final_mod, columna="centro_costo", n=5
)

consulta_final_mod_elim = tf.filtrar_por_valores(
    df=consulta_final_mod_elim, columna="centro_costo", valores=["#/#"], incluir=False
)


df_gastos_cn_replace = tf.Reemplazar_valores_con_dict_pd(
    df=df_gastos_cn,
    columna="centro_costo",
    diccionario_mapeo=config["reemplazos_gastos"],
)
df_gastos_cn_group = tf.Group_by_and_sum_cols_pd(
    df=df_gastos_cn_replace, group_col="centro_costo", sum_col=["total_gastos_cn"]
)

consulta_final_mod_gr_gas = tf.Group_by_and_sum_cols_pd(
    df=consulta_final_mod_elim, group_col="centro_costo", sum_col=["total_gastos_cn"]
)

df_compar_gastos = tf.pd_left_merge(
    base_left=df_gastos_cn_replace,
    base_right=consulta_final_mod_gr_gas,
    key="centro_costo",
    valid_suffixes=True,
    suffixes=("_insumo", "_Postgress"),
)

df_compar_gastos = calcular_diferencias_gastos(df_compar_gastos)

df_compar_gastos.to_excel("comparacion_gastos.xlsx", index=False)

gf.exportar_alertas_a_excel(
    alertas_dict=resultado_indiv_vs_indiv1,  # o el que tengas
    path_salida="alertas_comparativas.xlsx",
)
gf.exportar_alertas_a_excel(
    alertas_dict=resultado_combi_vs_combi1,  # o el que tengas
    path_salida="alertas_comparativas_comb.xlsx",
)


A = input("¿Esta trabajando con Real ó Presupuesto?: (R/P): ").upper()

if A == "P":
    nom_tabla = list_tablas_cxs[0]
else:
    nom_tabla = list_tablas_cxs[1]

db_manager = gf.DatabaseManager()

db_manager.insert_dataframe(
    dataframe=consulta_final_mod, table_name=nom_tabla, schema="cxs"
)
