# Importar librerias,  funciones  y constantes necesarias.
import pandas as pd
from General_Functions import Cargar_insumo, Eliminar_acentos
from config_constans import config
from config_constans import (
    PATH,
    TIPO_NO_DS,
    TIPO_DS,
    TIPO_CADENAS,
    CONSULTAS,
    VENTAS,
    VENTA_CADENAS,
    VENTA_COMP,
    NUM_SHEET,
    CONSULTA_DS,
    CONSULTA_NO_DS,
    CONSULTA_CADENAS,
    TIPO_VENTA_CADENAS,
    TIPO_VENTA_COMP,
)

# Obtener la ruta de la carpeta de consultas desde la configuración
ruta_consultas = config[PATH][CONSULTAS]
ruta_ventas = config[PATH][VENTAS]

# Obtener los números de hoja de las consultas para los tipos 'No_DS' y 'DS' , Cadenas
num_hojas_consulta_no_ds = config[CONSULTAS][TIPO_NO_DS][NUM_SHEET]
num_hojas_consulta_ds = config[CONSULTAS][TIPO_DS][NUM_SHEET]
num_hojas_consulta_cadenas = config[CONSULTAS][TIPO_CADENAS][NUM_SHEET]

# Obtener los detalles de la base de datos para las consultas 'No_DS', 'DS' y Cadenas
base_no_ds = config[CONSULTAS][TIPO_NO_DS]
base_ds = config[CONSULTAS][TIPO_DS]
base_cadenas = config[CONSULTAS][TIPO_CADENAS]

# Cargar las consultas del insumo (archivo) en las listas respectivas

consultas_no_ds = Cargar_insumo(ruta_consultas, base_no_ds, num_hojas_consulta_no_ds)
consultas_cadenas = Cargar_insumo(
    ruta_consultas, base_cadenas, num_hojas_consulta_cadenas
)
consultas_ds = Cargar_insumo(ruta_consultas, base_ds, num_hojas_consulta_ds)

#LLenar todos los NaN de Cadenas. (CanalT_SubCT_SegT_F (Vacios))
#consultas_cadenas[0][["Unnamed: 4","Unnamed: 5","Unnamed: 6","Unnamed: 8"]].fillna("Sin asignar", inplace=True)

# Crear un diccionario que almacena las consultas categorizadas por tipo


consultas_ds = [Eliminar_acentos(elemento.copy()) for elemento in consultas_ds]
consultas_no_ds = [Eliminar_acentos(elemento.copy()) for elemento in consultas_no_ds]
consultas_cadenas = [Eliminar_acentos(elemento.copy()) for elemento in consultas_cadenas]

consultas_por_tipo = {
    CONSULTA_DS: consultas_ds,
    CONSULTA_NO_DS: consultas_no_ds,
    CONSULTA_CADENAS: consultas_cadenas,
}

num_hojas_consulta_venta_cadenas = config[VENTAS][TIPO_VENTA_CADENAS][NUM_SHEET]
num_hojas_consulta_venta_general = config[VENTAS][TIPO_VENTA_COMP][NUM_SHEET]

base_venta_cadenas = config[VENTAS][TIPO_VENTA_CADENAS]
base_venta_general = config[VENTAS][TIPO_VENTA_COMP]

# Inicio del proceso de lectura de archivos ventas (Segunda parte para sacar documento
# del CxS ).

ventas_cadenas = Cargar_insumo(
    ruta_ventas, base_venta_cadenas, num_hojas_consulta_venta_cadenas
)
ventas_comp = Cargar_insumo(
    ruta_ventas, base_venta_general, num_hojas_consulta_venta_general
)

ventas_cadenas = [Eliminar_acentos(elemento.copy()) for elemento in ventas_cadenas]
ventas_comp = [Eliminar_acentos(elemento.copy()) for elemento in ventas_comp]


ventas_por_tipo = {
    VENTA_CADENAS : ventas_cadenas,
    VENTA_COMP : ventas_comp
}

# Proximo paso:
# Lectura de los drivers requeridos para el proceso.
# Limpieza y concatenación de los dataframes en el script => Limpieza_cocatenacion.py
