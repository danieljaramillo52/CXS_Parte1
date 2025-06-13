import pandas as pd
from loguru import logger
from General_Functions import registro_tiempo

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



