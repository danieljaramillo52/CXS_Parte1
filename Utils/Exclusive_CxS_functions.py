import pandas as pd
from loguru import logger
from General_Functions import registro_tiempo
from Trasformation_functions import Group_by_and_sum_cols_pd

@registro_tiempo
#Esa funcion es muy adaptable para otros programas revisarla.
def Reemplazos_multiples_columnas(
    df: pd.DataFrame, columnas_a_verificar: list, reemplazos: dict ,nom_condicion:str, col_clave:str
) -> pd.DataFrame:
    """
    Reemplaza los valores en múltiples columnas del DataFrame 'df' con los valores especificados
    en el diccionario 'reemplazos', solo si todas las columnas en 'columnas_a_verificar'
    tienen el valor "Sin asignar".

    Args:
        df (pd.DataFrame): DataFrame en el que se realizarán los reemplazos.
        columnas_a_verificar (list): Lista de nombres de columnas a verificar para "Sin asignar".
        reemplazos (dict): Diccionario que mapea valores de columna a reemplazar.

    Returns:
        pd.DataFrame: El DataFrame 'df' con los reemplazos realizados si se cumplen las condiciones.
    """
    for valor_columna, reemplazo in reemplazos.items():
        condicion = (df[columnas_a_verificar] == nom_condicion).all(axis=1) & (
            df[col_clave] == valor_columna
        )
        indices_a_remplazar = df[condicion].index
        df.loc[indices_a_remplazar, columnas_a_verificar] = reemplazo

    return df


#Función adaptable para muchos contextos.  
def Filtrar_DataFrames_ventas(df:pd.DataFrame, col_a_filtrar:str, val_a_filtrar:str, cols_necesarias:list)-> pd.DataFrame:
    """
    Filtra un DataFrame según un valor de columna y selecciona las columnas necesarias.

    Parameters:
        - df (DataFrame): El DataFrame que se va a filtrar.
        - col_filtrar (str): El nombre de la columna utilizada para el filtrado.
        - cols_necesarias (list): Lista de nombres de columnas necesarias.

    Returns:
        - DataFrame: El DataFrame resultante después del filtrado y selección de columnas.
    """
    try:
        logger.info("Inicio filtracion de la consulta de ventas.")
        resultado = df[~df[col_a_filtrar].isin([val_a_filtrar])][cols_necesarias]
        logger.success("Proceso terminado para la consulta de ventas: ")
        return resultado
    except Exception as e:
        logger.criticar(f"Error en la función FiltrarDataFrame: {e}")
        return None


def generar_agrupaciones(
    df_gn,
    df_gc,
    group_columns,
    sum_columns,
    incluir_formato_sector=False
):
    """
    Genera diccionarios de agrupaciones individuales y combinadas a partir de dos DataFrames
    diferenciados por la presencia o no de la columna 'formato'.

    Args:
        df_gn (pd.DataFrame): DataFrame general (sin desagregar por 'formato').
        df_gc (pd.DataFrame): DataFrame con desagregación por 'formato'.
        group_columns (List[str]): Lista de columnas a usar para agrupaciones.
        sum_columns (List[str]): Lista de columnas numéricas que se deben sumar.
        incluir_formato_sector (bool, optional): Si se debe incluir la combinación fija
            'formato/sector' como agrupación adicional. Default: False.

    Returns:
        Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
            - agrup_ind: agrupaciones individuales por cada columna.
            - agrup_comb: agrupaciones combinadas progresivas y opcionalmente la fija 'formato/sector'.
    """
    # Constantes
    COLUMNA_FORMATO = "formato"
    CLAVE_COMBINACION_FIJA = "oficina_ventas/sector"
    COLUMNAS_COMBINACION_FIJA = ["sector","oficina_ventas"]
    agrup_ind = {}
    agrup_comb = {}

    # Agrupaciones individuales
    for col in group_columns:
        df = df_gc if col == COLUMNA_FORMATO else df_gn
        agrup_ind[col] = Group_by_and_sum_cols_pd(
            df=df, group_col=col, sum_col=sum_columns
        )

    # Agrupaciones combinadas progresivas
    for i in range(2, len(group_columns)):
        combinacion = group_columns[:i]
        clave = "/".join(combinacion)
        df = df_gc if COLUMNA_FORMATO in combinacion else df_gn

        agrup_comb[clave] = df.groupby(combinacion, as_index=False)[sum_columns].sum()

    # Agrupación adicional fija
    if incluir_formato_sector:
        agrup_comb[CLAVE_COMBINACION_FIJA] = df_gc.groupby(
            COLUMNAS_COMBINACION_FIJA, as_index=False
        )[sum_columns].sum()

    return agrup_ind, agrup_comb


def calcular_diferencias_gastos(df):
    """
    Calcula la diferencia absoluta y relativa entre dos columnas de gastos
    y agrega una columna de alerta basada en un umbral de variación porcentual.

    Args:
        df (pd.DataFrame): DataFrame que contiene al menos las columnas de gastos:
            - total_gastos_cn_Postgress
            - total_gastos_cn_insumo

    Returns:
        pd.DataFrame: El mismo DataFrame con tres columnas adicionales:
            - diff_gastos: diferencia absoluta entre los gastos.
            - diff_gastos_pct: diferencia porcentual relativa al valor de Postgress.
            - alerta_gastos: booleano que indica si la diferencia supera el umbral.
    """
    # Constantes
    COL_GASTOS_POSTGRESS = "total_gastos_cn_Postgress"
    COL_GASTOS_INSUMO = "total_gastos_cn_insumo"
    COL_DIFERENCIA_ABS = "diff_gastos"
    COL_DIFERENCIA_PCT = "diff_gastos_pct"
    COL_ALERTA = "alerta_gastos"
    UMBRAL_ALERTA = 0.001

    df.loc[:, COL_DIFERENCIA_ABS] = (
        df[COL_GASTOS_POSTGRESS].astype(float) - df[COL_GASTOS_INSUMO].astype(float)
    ).abs()

    df.loc[:, COL_DIFERENCIA_PCT] = (
        df[COL_DIFERENCIA_ABS] / df[COL_GASTOS_POSTGRESS]
    )

    df.loc[:, COL_ALERTA] = df[COL_DIFERENCIA_PCT] > UMBRAL_ALERTA

    return df
