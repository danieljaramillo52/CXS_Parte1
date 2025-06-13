import pandas as pd
import numpy as np
from typing import Union
import copy
from loguru import logger
from typing import List, Any
from General_Functions import registro_tiempo


@registro_tiempo
def concatenate_dataframes_from_dict(data_dict: dict, bases_concatenadas={}) -> dict:
    """
    Concatena DataFrames desde un diccionario de listas de DataFrames.

    Args
    data_dict (dict): Un diccionario donde las claves representan tipos de consultas y los valores son listas de DataFrames.

    Returns:
    concatenated_dfs (dict): Un diccionario que contiene DataFrames concatenados para cada tipo de consulta.

    *Nota: Cuando usamos la paralabra "consulta", se hace alusión a un archivo de excel con extensiones .xlsx ó xlsm, o  archivo de texto plano .csv
    """
    try:
        logger.info(f"Proceso de concatenación de dataframes.")
        for key, lista_dataframes in data_dict.items():
            # Concatena los DataFrames del diccionario en un DataFrame
            df_concatenado = pd.concat(lista_dataframes, ignore_index=True)
            bases_concatenadas[key] = df_concatenado
    except Exception:
        logger.critical("Proceso de concatenación fallido {e}")
        raise Exception

    bases_concatenadas[key] = df_concatenado

    return bases_concatenadas


def eliminar_primeras_n_filas(data_dict: dict, n: int) -> dict:
    """
    Elimina las primeras n filas de cada DataFrame en las listas correspondientes a los valores de un diccionario.

    Args:
    data_dict (dict): Un diccionario donde las claves representan nombres de consultas y los valores son listas de DataFrames.
    n (int): Número de filas a eliminar.

    *Nota: Cuando usamos la paralabra "consulta", se hace alusión a un archivo de excel con extensiones .xlsx ó xlsm, o  archivo de texto plano .csv

    Returns:
    data_dict (dict): Un nuevo diccionario con las primeras n filas eliminadas de cada DataFrame.
    """
    try:
        logger.info(f"Eliminación de filas de todos los dataframes: ")

        # Crear una copia del diccionario para no modificar el original
        new_data_dict = data_dict.copy()

        for key, lista_dataframes in new_data_dict.items():
            for i, df in enumerate(lista_dataframes):
                new_data_dict[key][i] = df.iloc[n:]  # Elimina las primeras n filas
        logger.success("Eliminación de filas completa.")
    except Exception as e:
        logger.critical("Proceso de eliminación de filas fallido.")
        raise e
    return new_data_dict


@registro_tiempo
def Renombrar_columnas_con_diccionario(
    base: pd.DataFrame, cols_to_rename: dict
) -> pd.DataFrame:
    """Funcion que toma un diccionario con keys ( nombres actuales ) y values (nuevos nombres) para remplazar nombres de columnas en un dataframe.
    Args:
        base: dataframe al cual se le harán los remplazos
        cols_to_rename: diccionario con nombres antiguos y nuevos
    Result:
        base_renombrada: Base con las columnas renombradas.
    """
    base_renombrada = None

    try:
        base_renombrada = base.rename(columns=cols_to_rename, inplace=False)
        logger.success("Proceso de renombrar columnas satisfactorio: ")
    except Exception:
        logger.critical("Proceso de renombrar columnas fallido.")
        raise Exception

    return base_renombrada


def eliminar_fila_por_indice(df: pd.DataFrame, indice: int) -> pd.DataFrame:
    """
    Elimina una fila de un DataFrame por índice, con manejo de errores.

    Args:
        df (pd.DataFrame): El DataFrame original.
        indice (int): Índice de la fila a eliminar.

    Returns:
        pd.DataFrame: DataFrame sin la fila especificada, o el original si hay error.
    """
    try:
        nuevo_df = df.drop(index=indice)
        logger.info(f"Fila con índice {indice} eliminada correctamente.")
        return nuevo_df
    except KeyError:
        logger.error(f"Error: El índice {indice} no existe en el DataFrame.")
        return df


@registro_tiempo
def Reemplazar_columna_en_funcion_de_otra(
    df: pd.DataFrame,
    nom_columna_a_reemplazar: str,
    nom_columna_de_referencia: str,
    mapeo: dict,
) -> pd.DataFrame:
    """
    Reemplaza los valores en una columna en función de los valores en otra columna en un DataFrame.

    Args:
        df (pandas.DataFrame): El DataFrame en el que se realizarán los reemplazos.
        columna_a_reemplazar (str): El nombre de la columna que se reemplazará.
        columna_de_referencia (str): El nombre de la columna que se utilizará como referencia para el reemplazo.
        mapeo (dict): Un diccionario que mapea los valores de la columna de referencia a los nuevos valores.

    Returns:
        pandas.DataFrame: El DataFrame actualizado con los valores reemplazados en la columna indicada.
    """
    try:
        logger.info(f"Inicio de remplazamiento de datos en {nom_columna_a_reemplazar}")
        df[nom_columna_a_reemplazar] = np.where(
            df[nom_columna_de_referencia].isin(mapeo.keys()),
            df[nom_columna_de_referencia].map(mapeo),
            df[nom_columna_a_reemplazar],
        )
        logger.success(
            f"Proceso de remplazamiento en {nom_columna_a_reemplazar} exitoso"
        )
    except Exception as e:
        logger.critical(
            f"Proceso de remplazamiento de datos en {nom_columna_a_reemplazar} fallido."
        )
        raise e

    return df


def Group_by_and_sum_cols_pd(df=pd.DataFrame, group_col=list, sum_col=list):
        """
        Agrupa un DataFrame por una columna y calcula la suma de otra columna.

        Args:
            df (pandas.DataFrame): El DataFrame que se va a agrupar y sumar.
            group_col (list or str): El nombre de la columna o lista de nombres de columnas por la cual se va a agrupar.
            sum_col (list or str): El nombre de la columna o lista de nombres de columnas que se va a sumar.

        Returns:
            pandas.DataFrame: El DataFrame con las filas agrupadas y la suma calculada.
        """

        try:
            if isinstance(group_col, str):
                group_col = [group_col]

            if isinstance(sum_col, str):
                sum_col = [sum_col]

            result_df = df.groupby(group_col, as_index=False)[sum_col].sum()

            # Registro de éxito
            logger.info(f"Agrupación y suma realizadas con éxito en las columnas.")

        except Exception as e:
            # Registro de error crítico
            logger.critical(
                f"Error al realizar la agrupación y suma en las columnas. {e}"
            )
            result_df = None

        return result_df
    
def Remplazar_nulos_multiples_columnas(
    base: pd.DataFrame, list_columns: list, value: str
) -> pd.DataFrame:
    base_modificada = None
    """Funcion que toma un dataframe, una lista de sus columnas para hacer un 
    cambio en los datos nulos de las mismas.
    Args:
        base: Dataframe a base del cambio.
        list_columns: Columnas a modificar su tipo de dato.
        Value: valor del dato: (Notar, solo del tipo str.) 
    Returns: 
        base_modificada (copia de la base con los cambios.)
    """
    try:
        base.loc[:, list_columns] = base[list_columns].fillna(value)
        base_modificada = base
        logger.success("cambio tipo de dato satisfactorio: ")

    except Exception:
        logger.critical("cambio tipo de dato fallido.")
        raise Exception

    return base_modificada

def Asignar_col_nula(
    dict_consultas: dict, nom_consulta: str, list_columnas: list
) -> pd.DataFrame:
    """Funcion que toma una consulta de un diccionario, y a todas sus hojas les agrega una lista
    de columnas con valores nulos y nombres especificos.

    *Nota: Cuando usamos la paralabra "consulta", se hace alusión a un archivo de excel con extensiones .xlsx ó xlsm, o  archivo de texto plano .csv

    Args:
        dict_consultas (dict): diccionario de donde salen las consultas
        nom_consulta (str): clave del diccionario para la consutla especifica
        list_columnas (list): lista que contiene los nombre de las columnas a agregar.

    Returns:
        pd.DataFrame: dataframe modificado con las nuevas columnas agregadas.
    """

    from loguru import logger

    try:
        logger.info(
            f"Proceso para agregar las columnas {list_columnas} en la consulta {nom_consulta} iniciado:"
        )
        for cada_consulta in dict_consultas[nom_consulta]:
            for cada_columna in list_columnas:
                cada_consulta[cada_columna] = np.nan
        logger.success("Columnas asignadas con exito")
    except Exception as e:
        logger.critical(
            f"Proceso de agregar las columnas {list_columnas} en la consulta {nom_consulta} fallido: "
        )
        raise e
    return dict_consultas  # Devuelve el diccionario modificado

def Cambiar_tipo_dato_multiples_columnas_pd(
        base: pd.DataFrame, list_columns: list, type_data: type
    ) -> pd.DataFrame:
        """
        Función que toma un DataFrame, una lista de sus columnas para hacer un cambio en el tipo de dato de las mismas.

        Args:
            base (pd.DataFrame): DataFrame que es la base del cambio.
            list_columns (list): Columnas a modificar su tipo de dato.
            type_data (type): Tipo de dato al que se cambiarán las columnas (ejemplo: str, int, float).

        Returns:
            pd.DataFrame: Copia del DataFrame con los cambios.
        """
        try:
            # Verificar que el DataFrame tenga las columnas especificadas
            for columna in list_columns:
                if columna not in base.columns:
                    raise KeyError(f"La columna '{columna}' no existe en el DataFrame.")

            # Cambiar el tipo de dato de las columnas
            base_copy = (
                base.copy()
            )  # Crear una copia para evitar problemas de SettingWithCopyWarning
            base_copy[list_columns] = base_copy[list_columns].astype(type_data)

            return base_copy

        except Exception as e:
            logger.critical(f"Error en Cambiar_tipo_dato_multiples_columnas: {e}")
            
def Reemplazar_valores_con_dict_pd(
        df: pd.DataFrame, columna: str, diccionario_mapeo: dict
    ):
        """
        Reemplaza los valores en la columna especificada de un DataFrame según un diccionario de mapeo.

        Args:
        - df (pd.DataFrame): El DataFrame a modificar.
        - columna (str): El nombre de la columna que se va a reemplazar.
        - diccionario_mapeo (dict): Un diccionario que define la relación de mapeo de valores antiguos a nuevos.

        Returns:
        - pd.DataFrame: El DataFrame modificado con los valores de la columna especificada reemplazados.

        - TypeError: Si 'df' no es un DataFrame de pandas o 'diccionario_mapeo' no es un diccionario.
        - KeyError: Si la 'columna' especificada no se encuentra en el DataFrame.

        """
        try:
            # Verificar si la entrada es un DataFrame de pandas
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

            # Verificar si la columna especificada existe en el DataFrame
            if columna not in df.columns:
                raise KeyError(f"Columna '{columna}' no encontrada en el DataFrame.")

            # Verificar si el diccionario de mapeo es un diccionario
            if not isinstance(diccionario_mapeo, dict):
                raise TypeError("'diccionario_mapeo' debe ser un diccionario.")

            # Realizar el reemplazo según el diccionario de mapeo
            df.loc[:, columna] = df[columna].replace(diccionario_mapeo)
            
            # Registrar mensaje de éxito
            logger.success(
                f"Valores de la columna '{columna}' reemplazados según el diccionario de mapeo."
            )

            return df

        except Exception as e:
            # Registrar mensaje crítico con detalles del tipo de error
            logger.critical(
                f"Error durante el reemplazo de valores en la columna. Tipo de error: {type(e).__name__}. Detalles: {str(e)}"
            )
            return None           
            
@registro_tiempo
def modificaciones_elegidas_dataframe(
    diccionario: dict, clave: str, columnas: list, operacion: str
) -> dict:
    """
    Modifica un DataFrame en un diccionario realizando una operación en las columnas especificadas.

    Args:
    - diccionario (dict): El diccionario que contiene los DataFrames.
    - clave (str): La clave del DataFrame dentro del diccionario que se modificará.
    - columnas (list): Una lista de las columnas en las que se realizará la operación.
    - operacion (str): La operación a realizar ('astype', 'fillna', 'drop').

    Returns:
    - diccionario modificado (dict)
    """
    # Crear una copia independiente del DataFrame
    df_for_copy = diccionario[clave]
    df_copy = df_for_copy.copy()

    try:
        # Realizar la operación especificada en las columnas
        logger.info(f"Inicio de la operacion {operacion}")
        if operacion == "astype":
            df_copy[columnas] = df_copy[columnas].astype(float)
        elif operacion == "fillna":
            df_copy[columnas] = df_copy[columnas].fillna(0)
        # Agregar más operaciones según sea necesario

        # Asignar la copia de nuevo al diccionario con la misma clave
        diccionario[clave] = df_copy
        logger.success(f"operacion {operacion} completada con exito")

    except Exception as e:
        logger.critical(f"Operacion {operacion} fallida en el dataframe {df_copy}")
        raise e
    return diccionario


# Función para concatenar valores de columnas en un DataFrame
@registro_tiempo
def concatenar_columnas(
    df: pd.DataFrame, columna1: str, columna2: str, nueva_columna: str
) -> None:
    """
    Concatena los valores de dos columnas en un DataFrame y agrega el resultado como una nueva columna.

    Args:
        df (pd.DataFrame): El DataFrame en el que se realizará la concatenación.
        columna1 (str): El nombre de la primera columna a concatenar.
        columna2 (str): El nombre de la segunda columna a concatenar.
        nueva_columna (str): El nombre de la nueva columna resultante.

    Returns:
        None
    """
    try:
        df[nueva_columna] = df[columna1].str.cat(df[columna2], sep="")
        logger.info(f"Se realizó la concatenación en el DataFrame correctamente.")
    except Exception as e:
        logger.critical(
            f"Error al realizar la concatenación en el DataFrame : {str(e)}"
        )

    return df


import pandas as pd
from loguru import logger  # Asegúrate de importar loguru


def reemplazar_indices_dataframe(
    daframe_escogido: pd.DataFrame,
    condicion: pd.Series,
    remplazo: dict,
    columna_actual: str,
    columnas_reemplazo: list,
):
    """
    Realiza el reemplazo de valores en un DataFrame bajo ciertas condiciones.

    Args:
        consultas_mod (pd.DataFrame): DataFrame en el que se realizarán los reemplazos.
        condicion (pd.Series): Serie booleana que representa la condición para realizar el reemplazo.
        remplazo (dict): Diccionario que mapea valores de columna a reemplazar.
        columna_actual (str): El nombre de la columna actual en la que se realizará el reemplazo.
        columnas_reemplazo (list): Lista de nombres de columnas en las que se realizará el reemplazo.

    Returns:
        None
    """
    try:
        indices_a_remplazar = daframe_escogido[condicion].index
        daframe_escogido.loc[indices_a_remplazar, columnas_reemplazo] = remplazo

        # Registro de éxito
        logger.success(
            f"Reemplazo exitoso para {len(indices_a_remplazar)} filas en la columna {columna_actual}"
        )

    except Exception as e:
        # Registro de error crítico en caso de excepción
        logger.critical(
            f"Error crítico al realizar el reemplazo para la columna {columna_actual}: {str(e)}"
        )

def comparar_diccionarios_df(
    dict1: dict, dict2: dict, sum_columns: list, umbral: float = 0.005
) -> dict:
    resultados = {}

    claves_comunes = set(dict1.keys()).intersection(dict2.keys())

    for clave in claves_comunes:
        df1 = dict1[clave].copy()
        df2 = dict2[clave].copy()

        columnas_join = clave.split("/")  # columnas de agrupación deducidas

        df_merged = pd.merge(
            df1, df2, on=columnas_join, suffixes=("_Postgress", "_insumo")
        )

        for col_sum in sum_columns:
            col_df1 = f"{col_sum}_Postgress"
            col_df2 = f"{col_sum}_insumo"
            diff_abs = f"{col_sum[0:3]}_diff_abs"
            diff_pct = f"{col_sum[0:3]}_diff_pct"
            alerta = f"{col_sum[0:3]}_alerta"

            df_merged[diff_abs] = (df_merged[col_df1] - df_merged[col_df2]).abs()
            df_merged[diff_pct] = df_merged[diff_abs] / df_merged[col_df1].replace(0, 1)
            df_merged[alerta] = df_merged[diff_pct] > umbral

        df_merged_rn = Renombrar_columnas_con_diccionario(
            base=df_merged,
            cols_to_rename={
                col_df1: f"{col_df1[0:3]}_Postgress",
                col_df2: f"{col_df2[0:3]}_insumo",
            },
        )

        resultados[clave] = df_merged_rn

    return resultados

def pd_left_merge(
    base_left: pd.DataFrame, base_right: pd.DataFrame, key: str | List[str], valid_suffixes: bool = False, suffixes: str | None = None
) -> pd.DataFrame:
    """Función que retorna el left join de dos dataframe de pandas.

    Args:
        base_left (pd.DataFrame): Dataframe que será la base del join.
        base_right (pd.DataFrame): Dataframe del cuál se extraerá la información complementaria.
        key (str): Llave mediante la cual se va a realizar el merge o join.

    Returns:
        pd.DataFrame: Dataframe con el merge de las dos fuentes de datos.
    """

    # Validar que base_left y base_right sean DataFrames de pandas
    if not isinstance(base_left, pd.DataFrame):
        raise ValueError("El argumento base_left no es un DataFrame de pandas")
    if not isinstance(base_right, pd.DataFrame):
        raise ValueError("El argumento base_right no es un DataFrame de pandas")

    base = None

    try:
        if valid_suffixes:
            base = pd.merge(left=base_left, right=base_right, how="left", on=key, suffixes=suffixes)
        else:
            base = pd.merge(left=base_left, right=base_right, how="left", on=key)
        logger.success("Proceso de merge satisfactorio")
    except pd.errors.MergeError as e:
        logger.critical(f"Proceso de merge fallido: {e}")
        raise e

    return base



def filtrar_por_valores(
    df: pd.DataFrame, columna: str, valores: list[str | int], incluir: bool = True
) -> pd.DataFrame | None:
    """
    Filtra un DataFrame incluyendo o excluyendo filas según los valores en una columna.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a filtrar.
    columna : str
        Nombre de la columna sobre la cual aplicar el filtro.
    valores : list of str or int
        Lista de valores a incluir o excluir.
    incluir : bool, default=True
        Si True, incluye las filas con los valores indicados. Si False, las excluye.

    Returns
    -------
    pd.DataFrame or None
        DataFrame filtrado o None si ocurre un error.
    """
    try:
        if isinstance(valores, (str, int)):
            valores = [valores]

        if incluir:
            df_filtrado = df[df[columna].isin(valores)]
        else:
            df_filtrado = df[~df[columna].isin(valores)]

        return df_filtrado

    except Exception as e:
        logger.critical(f"Error al filtrar por valores en la columna '{columna}': {e}")
        return None

def pd_left_merge_two_keys(
    base_left: pd.DataFrame,
    base_right: pd.DataFrame,
    left_key: str,
    right_key: str,
) -> pd.DataFrame:
    """Función que retorna el left join de dos dataframe de pandas.

    Args:
        base_left (pd.DataFrame): Dataframe que será la base del join.
        base_right (pd.DataFrame): Dataframe del cuál se extraerá la información complementaria.
        key (str): Llave mediante la cual se va a realizar el merge o join.

    Returns:
        pd.DataFrame: Dataframe con el merge de las dos fuentes de datos.
    """

    # Validar que base_left y base_right sean DataFrames de pandas
    if not isinstance(base_left, pd.DataFrame):
        raise ValueError("El argumento base_left no es un DataFrame de pandas")
    if not isinstance(base_right, pd.DataFrame):
        raise ValueError("El argumento base_right no es un DataFrame de pandas")

    base = None

    try:
        base = pd.merge(
            left=base_left,
            right=base_right,
            how="left",
            left_on=left_key,
            right_on=right_key,
        )
        logger.success("Proceso de merge satisfactorio")
    except pd.errors.MergeError as e:
        logger.critical(f"Proceso de merge fallido: {e}")
        raise e

    return base


def merge_inner_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, on_column: str
) -> pd.DataFrame:
    """
    Realiza un merge inner de dos DataFrames en pandas basándose en una columna común.

    Parámetros:
    df1 (pd.DataFrame): Primer DataFrame.
    df2 (pd.DataFrame): Segundo DataFrame.
    on_column (str): Nombre de la columna sobre la cual realizar el merge inner.

    Retorna:
    pd.DataFrame: DataFrame resultante del merge inner.
    """
    # Verificar si df1 y df2 son instancias de pd.DataFrame
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise TypeError(
            "Los argumentos df1 y df2 deben ser instancias de pd.DataFrame."
        )

    try:
        result = pd.merge(df1, df2, on=on_column, how="inner")
        logger.info(
            f"Merge inner completado correctamente en la columna '{on_column}'."
        )
        return result
    except Exception as e:
        logger.critical(f"Error al realizar el merge inner: {str(e)}")
        return None

def eliminar_primeros_n_caracteres(
    df: pd.DataFrame, columna: str, n: int
) -> pd.DataFrame:
    """
    Elimina los primeros n caracteres de cada fila en la columna especificada del DataFrame.

    Parámetros:
    df (pd.DataFrame): DataFrame a modificar.
    columna (str): Nombre de la columna en la que se realizará la operación.
    n (int): Número de caracteres a eliminar de cada fila en la columna.

    Retorna:
    pd.DataFrame: DataFrame con la columna modificada.
    """
    try:
        # Verificar si df es un DataFrame de pandas y columna es una columna válida en df
        if not isinstance(df, pd.DataFrame):
            raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")
        if columna not in df.columns:
            raise ValueError(f"La columna '{columna}' no existe en el DataFrame.")

        df_copy = df.copy()
        # Modificar la columna en el DataFrame
        df_copy[columna] = df_copy[columna].apply(
            lambda x: x[n:] if isinstance(x, str) and len(x) > n else x
        )

        # Registrar el proceso
        logger.info(
            f"Se eliminaron los primeros {n} caracteres de la columna '{columna}'."
        )

        return df_copy

    except Exception as e:
        # Registrar el error y retornar None en caso de fallo
        logger.critical(
            f"Error al eliminar los primeros {n} caracteres de la columna '{columna}': {str(e)}"
        )
        return None


def eliminar_espacios_finales(
    df: pd.DataFrame, columna: str
) -> Union[pd.DataFrame, None]:
    """
    Elimina los espacios en blanco al final de los datos en una columna específica del DataFrame.

    Parámetros:
    df (pd.DataFrame): DataFrame a modificar.
    columna (str): Nombre de la columna en la que se realizará la operación.

    Retorna:
    pd.DataFrame: DataFrame con la columna modificada.
    """
    try:
        # Verificar si df es un DataFrame de pandas y columna es una columna válida en df
        if not isinstance(df, pd.DataFrame):
            raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")
        if columna not in df.columns:
            raise ValueError(f"La columna '{columna}' no existe en el DataFrame.")

        # Aplicar la operación para eliminar espacios en blanco al final
        df[columna] = df[columna].str.rstrip()

        # Registrar el proceso
        logger.info(
            f"Se eliminaron los espacios en blanco al final de la columna '{columna}'."
        )

        return df

    except Exception as e:
        # Registrar el error y retornar None en caso de fallo
        logger.critical(
            f"Error al eliminar espacios en blanco al final de la columna '{columna}': {str(e)}"
        )
        return None



