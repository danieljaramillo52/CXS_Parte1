# En este modulo se hace la lectura y procesamiento de los drivers.
from General_Functions import (
    Lectura_insumos_excel,
    Procesar_configuracion,
    Eliminar_acentos,
)

# Procesamos la configuración del proyecto.
config = Procesar_configuracion(nom_archivo_configuracion="config.yml")
# Configurar drivers.
ruta = config["path"]["Drivers"]

driver_no_ds = Lectura_insumos_excel(
    path=ruta,
    nom_insumo=config["Drivers"]["Driver_NO_DS"]["file_name"],
    nom_Hoja=config["Drivers"]["Driver_NO_DS"]["sheet"],
    cols=config["Drivers"]["Driver_NO_DS"]["cols"],
)

driver_ds = Lectura_insumos_excel(
    path=ruta,
    nom_insumo=config["Drivers"]["Driver_DS"]["file_name"],
    nom_Hoja=config["Drivers"]["Driver_DS"]["sheet"],
    cols=config["Drivers"]["Driver_DS"]["cols"],
)

ruta = config["path"]["Drivers"]
driver_cadenas = Lectura_insumos_excel(
    path=ruta,
    nom_insumo=config["Drivers"]["Driver_Cadenas"]["file_name"],
    nom_Hoja=config["Drivers"]["Driver_Cadenas"]["sheet"],
    cols=config["Drivers"]["Driver_Cadenas"]["cols"],
)

# Eliminar acentos de drivers.

# Los tres drivers se importan al script config_dicts donde se procesa con la información de los drivers, los diccionarios requeridos para la automatización.
# Proximo paso:
driver_cadenas = Eliminar_acentos(driver_cadenas.copy())
driver_ds = Eliminar_acentos(driver_ds.copy())
driver_no_ds = Eliminar_acentos(driver_no_ds.copy())

# Limpieza y concatenación de los dataframes en el script => Limpieza_cocatenacion.py
