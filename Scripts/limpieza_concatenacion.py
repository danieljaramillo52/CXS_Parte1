# Importación de librerias y variables
from lectura_consultas_ventas import consultas_por_tipo
from config_constans import config
from Trasformation_functions import (
    eliminar_primeras_n_filas,
    Renombrar_columnas_con_diccionario,
    concatenate_dataframes_from_dict,
    Asignar_col_nula,
)

# Importación de constantes. #Consultar en el modulo lectura.py
from config_constans import (
    TIPO_CADENAS,
    TIPO_DS,
    TIPO_NO_DS,
    CONSULTAS,
    CONSULTA_DS,
    CONSULTA_NO_DS,
    CONSULTA_CADENAS,
    COLS_AGREGAR,
    COLS_NUEVAS,
)

# Eliminación de las dos primera filas en todos los dataframes.
consultas_por_tipo_Mod = eliminar_primeras_n_filas(consultas_por_tipo, 2)

# Agregar columnas adicionales a cada consulta.

# Nombres de las columnas a agregar parametrizados
cols_agregar_ds = config[CONSULTAS][TIPO_DS][COLS_AGREGAR]
cols_agregar_no_ds = config[CONSULTAS][TIPO_NO_DS][COLS_AGREGAR]
cols_agregar_cadenas = config[CONSULTAS][TIPO_CADENAS][COLS_AGREGAR]

# El mismo diccionario se ira modificando dependiendo de la consulta requerida.
consultas_por_tipo_Mod = Asignar_col_nula(
    dict_consultas=consultas_por_tipo_Mod,
    nom_consulta=CONSULTA_NO_DS,
    list_columnas=cols_agregar_no_ds,
)
# A cada instacia de la función le pasamos el diccionario sobreescrito.
consultas_por_tipo_Mod = Asignar_col_nula(
    dict_consultas=consultas_por_tipo_Mod,
    nom_consulta=CONSULTA_DS,
    list_columnas=cols_agregar_ds,
)

consultas_por_tipo_Mod = Asignar_col_nula(
    dict_consultas=consultas_por_tipo_Mod,
    nom_consulta=CONSULTA_CADENAS,
    list_columnas=cols_agregar_cadenas,
)

# Eliminar columna adicional num_ofina_ventas de base_ds:
for cada_consulta in consultas_por_tipo_Mod[CONSULTA_DS]:
    cada_consulta.drop(columns=["Unnamed: 7"], inplace=True)

# Renombrar columnas con diccionario.
# Parámetros para cada llamada a la función para renombrar las cols.
parametros_por_clave = {
    CONSULTA_DS: (config[CONSULTAS][TIPO_DS][COLS_NUEVAS]),
    CONSULTA_NO_DS: (config[CONSULTAS][TIPO_NO_DS][COLS_NUEVAS]),
    CONSULTA_CADENAS: (config[CONSULTAS][TIPO_CADENAS][COLS_NUEVAS]),
}

# Recorrer el diccionario y aplicar la función de renombrar a cada DataFrame
for clave, lista_dataframes in consultas_por_tipo_Mod.items():
    parametros = parametros_por_clave[clave]
    for i, df in enumerate(lista_dataframes):
        consultas_por_tipo_Mod[clave][i] = Renombrar_columnas_con_diccionario(
            df, parametros
        )

# Concatenar los dataframes de cada tipo.
consul_concat_por_tipo = concatenate_dataframes_from_dict(consultas_por_tipo_Mod)

# Reordenar columnas de los dataframes.
consul_concat_por_tipo[CONSULTA_DS] = consul_concat_por_tipo[CONSULTA_DS][
    list(parametros_por_clave[CONSULTA_DS].values())
]

consul_concat_por_tipo[CONSULTA_NO_DS] = consul_concat_por_tipo[CONSULTA_NO_DS][
    list(parametros_por_clave[CONSULTA_NO_DS].values())
]

consul_concat_por_tipo[CONSULTA_CADENAS] = consul_concat_por_tipo[CONSULTA_CADENAS][
    list(parametros_por_clave[CONSULTA_CADENAS].values())
]

insumo_consultas = consul_concat_por_tipo

from pandas import concat
df_consultas_completo = concat(insumo_consultas.values(), axis=0)

#df_consultas_completo["Ventas_Netas_CN"] = df_consultas_completo["Ventas_Efectivas"].#astype(float).fillna(0) - df_consultas_completo["Descuentos"].astype(float).fillna(0)