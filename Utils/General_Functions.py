import os
import re
import yaml
import time
import pandas as pd
from loguru import logger
from unidecode import unidecode
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd


def Procesar_configuracion(nom_archivo_configuracion: str) -> dict:
    """Lee un archivo YAML de configuración para un proyecto.

    Args:
        nom_archivo_configuracion (str): Nombre del archivo YAML que contiene
            la configuración del proyecto.

    Returns:
        dict: Un diccionario con la información de configuración leída del archivo YAML.
    """
    try:
        with open(nom_archivo_configuracion) as ymlfile:
            configuracion = yaml.full_load(ymlfile)
        logger.success("Proceso de obtención de configuración satisfactorio")
    except Exception as e:
        logger.critical(f"Proceso de lectura de configuración fallido {e}")
        raise e

    return configuracion


def registro_tiempo(original_func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = original_func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(
            f"Tiempo de ejecución de {original_func.__name__}: {execution_time} segundos"
        )
        return result

    return wrapper


@registro_tiempo
def Lectura_insumos_excel(path, nom_insumo, nom_Hoja, cols):
    """Lee archivos de Excel con cualquier extensión y carga los datos de una hoja específica.

    Lee el archivo especificado por `nom_insumo` ubicado en la ruta `path` y carga los datos de la hoja
    especificada por `nom_Hoja`. Selecciona solo las columnas indicadas por `cols`.

    Args:
        path (str): Ruta de la carpeta donde se encuentra el archivo.
        nom_insumo (str): Nombre del archivo con extensión.
        nom_Hoja (str): Nombre de la hoja del archivo que se quiere leer.
        cols (int): Número de columnas que se desean cargar.

    Returns:
        pd.DataFrame: Dataframe que contiene los datos leídos del archivo Excel.

    Raises:
        Exception: Si ocurre un error durante el proceso de lectura del archivo.
    """
    base_leida = None

    try:
        logger.info(f"Inicio lectura {nom_insumo} Hoja {nom_Hoja}")
        base_leida = pd.read_excel(
            path + nom_insumo,
            sheet_name=nom_Hoja,
            usecols=list(range(0, cols)),
            dtype=str,
        )

        logger.success(
            f"Lectura de {nom_insumo} Hoja: {nom_Hoja} realizada con éxito"
        )  # Se registrará correctamente con el método "success"
    except Exception as e:
        logger.error(f"Proceso de lectura fallido: {e}")
        raise Exception

    return base_leida


@registro_tiempo
def pd_left_merge(
    base_left: pd.DataFrame, base_right: pd.DataFrame, key: str
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
        base = pd.merge(left=base_left, right=base_right, how="left", on=key)
        logger.success("Proceso de merge satisfactorio")
    except pd.errors.MergeError as e:
        logger.critical(f"Proceso de merge fallido: {e}")
        raise e

    return base


@registro_tiempo
def Cargar_insumo(ruta: str, base_config: str, num_hojas_consulta: int) -> list:
    """
    Carga consultas a partir de una configuración base en un directorio específico.

    Args:
        ruta (str): Ruta al directorio donde se encuentran los archivos de consulta.
        base_config (dict): Configuración base para las consultas, incluyendo nombre de archivo,
                           hojas y columnas.
        num_hojas_consulta (int): Número de hojas de consulta a cargar.

    Returns:
        list: Lista de consultas cargadas.

    Example:
        base_no_ds = {
            "file_name": "archivo_no_ds.xlsx",
            "sheet": ["hoja1", "hoja2"],
            "cols": [4, 5] => Indica que "hoja1" tiene 4 columnas, la "hoja2" 5
        }
        num_hojas_no_ds = len(base_no_ds["sheet"])
        consultas_no_ds = cargar_consultas(ruta, base_no_ds, num_hojas_no_ds)

    """
    try:
        consultas = []
        for i in range(0, num_hojas_consulta):
            consulta = Lectura_insumos_excel(
                path=ruta,
                nom_insumo=base_config["file_name"],
                nom_Hoja=base_config["sheet"][i],
                cols=base_config["cols"][i],
            )
            consultas.append(consulta)
    except Exception as e:
        logger.error(f"Proceso de lectura fallido: {e}")
        raise e

    return consultas


def Crear_diccionario_con_listas(dataframe: pd.DataFrame, col_clave: str) -> dict:
    """
    Crea un diccionario donde las claves son los valores de la columna 1
    y los valores son listas de los valores de las columnas 3 en adelante.

    Args:
        dataframe (pd.DataFrame): El DataFrame de entrada con las columnas deseadas.

    Returns:
        dict: Un diccionario con listas de valores de las columnas 1 en adelante.
        OJO: EL orden de las columnas importa. Ya que la columa 0 se usa como clave.
        Y no entra en el dict.
    """
    diccionario = {}
    try:
        for index, row in dataframe.iterrows():
            clave = row[col_clave]
            valores = list(row.iloc[1:])

            diccionario[clave] = valores

    except KeyError as e:
        logger.critical(f"Creación del diccionario fallida debido a una KeyError: {e}")
    except Exception as e:
        logger.critical(f"Creación del diccionario fallida. Error: {e}")

    return diccionario


def limpiar_nombre_hoja(nombre: str) -> str:
    # Excel no permite: []:*?/\
    return re.sub(r"[\\/*?:\[\]]", "_", nombre)[:31]  # Máx 31 caracteres


def exportar_alertas_a_excel(alertas_dict: dict, path_salida: str):
    with pd.ExcelWriter(path_salida, engine="xlsxwriter") as writer:
        for clave, df in alertas_dict.items():
            if df.empty:
                continue

            # Limpiar el nombre de la hoja
            nombre_hoja = limpiar_nombre_hoja(clave)

            # Guardar hoja detallada
            df.to_excel(writer, sheet_name=nombre_hoja, index=False)

def Group_by_and_sum(df=pd.DataFrame, group_col=str, sum_col=str):
    """
    Agrupa un DataFrame por una columna y calcula la suma de otra columna.

    Args:
        df (pandas.DataFrame): El DataFrame que se va a agrupar y sumar.
        group_col (str): El nombre de la columna por la cual se va a agrupar.
        sum_col (str): El nombre de la columna que se va a sumar.

    Returns:
        pandas.DataFrame: El DataFrame con las filas agrupadas y la suma calculada.
    """
    try:
        result_df = df.groupby(group_col, as_index=False)[sum_col].sum()

        # Registro de éxito
        logger.info(
            f"Agrupación y suma realizadas con éxito en las columnas {group_col} y {sum_col}."
        )

    except Exception as e:
        # Registro de error crítico
        logger.critical(
            f"Error al realizar la agrupación y suma en las columnas {group_col} y {sum_col}: {e}"
        )
        result_df = None

    return result_df

@registro_tiempo
def Eliminar_acentos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina las tildes y caracteres acentuados de todas las columnas de un DataFrame.

    Args:
        df (pd.DataFrame): El DataFrame en el que se eliminarán los acentos.

    Returns:
        pd.DataFrame: El DataFrame modificado con los acentos eliminados.
    """
    try:
        logger.info("Iniciando proceso de eliminación de acentos...")
        for columna in df.columns:
            # Verifica si la columna es de tipo objeto (texto)
            if df[columna].dtype == "object":
                # Aplica unidecode a cada valor de la columna y asigna los resultados de nuevo a la columna
                df[columna] = df[columna].apply(
                    lambda x: unidecode(x) if pd.notna(x) else x
                )
        logger.success("Proceso de eliminación de acentos completado con éxito.")
        return df
    except Exception as e:
        # Manejo de excepciones: registra un mensaje crítico en lugar de imprimir el error
        logger.critical(f"Error en la función eliminar_acentos: {str(e)}")
        return df



class DatabaseManager:
    """
    Clase para gestionar la conexión y operaciones con PostgreSQL 
    utilizando psycopg2 y SQLAlchemy.

    Esta clase permite establecer conexiones a la base de datos, 
    crear motores de conexión con SQLAlchemy y realizar inserciones de datos en tablas.
    """

    def __init__(self):
        """
        Inicializa el gestor de base de datos cargando las variables de entorno
        y configurando los parámetros de conexión.
        """
        load_dotenv()  # Carga las variables de entorno desde un archivo .env

        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT")

    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Establece y devuelve una conexión a la base de datos PostgreSQL.

        Returns:
            psycopg2.extensions.connection: Objeto de conexión a la base de datos.

        Raises:
            psycopg2.DatabaseError: Si ocurre un error en la conexión.
        """
        try:
            return psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
        except psycopg2.DatabaseError as e:
            raise Exception(f"Error al conectar con la base de datos: {e}")

    def create_engine(self):
        """
        Crea y devuelve un motor SQLAlchemy para interactuar con la base de datos.

        Returns:
            sqlalchemy.engine.base.Engine: Motor de conexión SQLAlchemy.
        """
        return create_engine(
            "postgresql+psycopg2://",
            creator=self.get_connection
        )

    def insert_dataframe(self, dataframe: pd.DataFrame, table_name: str, schema: str = "public") -> None:
        """
        Inserta un DataFrame en una tabla específica de la base de datos.

        Args:
            dataframe (pd.DataFrame): DataFrame con los datos a insertar.
            table_name (str): Nombre de la tabla en la que se insertarán los datos.
            schema (str, optional): Esquema en el que se encuentra la tabla. Por defecto es 'public'.

        Raises:
            ValueError: Si el DataFrame está vacío.
            Exception: Si ocurre un error en la inserción de datos.
        """
        if dataframe.empty:
            raise ValueError("El DataFrame está vacío y no se puede insertar en la base de datos.")

        try:
            engine = self.create_engine()
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                schema=schema,
                index=False
            )
        except Exception as e:
            raise Exception(f"Error al insertar datos en la tabla {table_name}: {e}")
    
    def read_table(self, table_name: str, schema: str = "public") -> pd.DataFrame:
        """
        Lee una tabla de PostgreSQL y la devuelve como un DataFrame de pandas.

        Args:
            table_name (str): Nombre de la tabla que se desea leer.
            schema (str, optional): Esquema en el que se encuentra la tabla. Por defecto es 'public'.

        Returns:
            pd.DataFrame: DataFrame con los datos de la tabla.

        Raises:
            Exception: Si ocurre un error al leer los datos.
        """
        try:
            engine = self.create_engine()
            query = f'SELECT * FROM "{schema}"."{table_name}"'  # Consulta SQL segura
            df = pd.read_sql(query, con=engine)
            return df
        except Exception as e:
            raise Exception(f"Error al leer la tabla {table_name}: {e}")