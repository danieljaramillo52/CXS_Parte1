from General_Functions import Procesar_configuracion

# Procesamos la configuración del proyecto.
config = Procesar_configuracion(nom_archivo_configuracion="config.yml")

# Rutas y ubicaciones
PATH = config["PATH"]

# Tipos de consultas
UNION = config["UNION"]
TIPO_NO_DS = config["TIPO_NO_DS"]
TIPO_DS = config["TIPO_DS"]
TIPO_CADENAS = config["TIPO_CADENAS"]

# Columnas para agregar
COLS_AGREGAR = config["COLS_AGREGAR"]

# Nombres de categorías
CONSULTAS = config["CONSULTAS"]
VENTAS = config["VENTAS"]

# Número de hoja o sheet
NUM_SHEET = config["NUM_SHEET"]

# Nombres de consultas específicas
CONSULTA_DS = config["CONSULTA_DS"]
CONSULTA_NO_DS = config["CONSULTA_NO_DS"]
CONSULTA_CADENAS = config["CONSULTA_CADENAS"]

# Nombres de nuevas columnas
COLS_NUEVAS = config["COLS_NUEVAS"]

# Nombres de segmentos y categorías
CENTRO_COS = config["CENTRO_COS"]
NOM_CENTRO_COS = config["NOM_CENTRO_COS"]
CONCATENADA = config["CONCATENADA"]
CANAL = config["CANAL"]
RAMO_CLAVE = config["RAMO_CLAVE"]
SUBCANAL = config["SUBCANAL"]

# Nombres de canales y segmentos de transformación
CANAL_TRANS = config["CANAL_TRANS"]
SUBCANAL_TRANS = config["SUBCANAL_TRANS"]
SEGMENTO_TRANS = config["SEGMENTO_TRANS"]

# Formatos y clasificaciones
FORMATO = config["FORMATO"]
FORMATO_NIF = config["FORMATO_NIF"]

# Nombres de columnas de segmento
COLS_CNAL_SUBCNAL_SEG = config["COLS_CNAL_SUBCNAL_SEG"]
COLS_CNAL_SUBCNAL_SEG_FNIF = config["COLS_CNAL_SUBCNAL_SEG_FNIF"]

# Valores especiales
FIL_SIN_ASIGNAR = config["FIL_SIN_ASIGNAR"]
FIL_NULL = config["FIL_NULL"]
FIL_DOBLE_NUMERAL = config["FIL_DOBLE_NUMERAL"]
SEGMENTO_TRANS = config["SEGMENTO_TRANS"]
SEGMENTO_AGRUP = config["SEGMENTO_AGRUP"]

# Columnas concatenadas
COLS_CONCAT_CADENAS = config["COLS_CONCAT_CADENAS"]

# Otros
NIF = config["NIF"]
FORMAT = config["FORMAT"]
CLIENTE_CET = config["CLIENTE_CET"]
CLIENTE = config["CLIENTE"]
CLASIFICACION = config["CLASIFICACION"]

# Diccionario de reemplazo para consultas DS
DICT_REMP_CNAL_SUBC_SEG_DS = config["DICT_REMP_CNAL_SUBC_SEG_DS"]

# Clientes y distribuidores
DISTRIBUIDORES = config["DISTRIBUIDORES"]
TRADICIONAL = config["TRADICIONAL"]

# Valores especiales
FIL_NUMERAL = config["FIL_NUMERAL"]

# Columnas necesarias para ventas
COLS_NECESARIAS = config["COLS_NECESARIAS"]
COLS_FOR_GROUPBY_SECOS= config["COLS_FOR_GROUPBY_SECOS"]
COLS_FOR_SUM = config["COLS_FOR_SUM"]
COLS_FOR_GROUPBY_DN = config["COLS_FOR_GROUPBY_DN"]
COLS_FOR_SUM_DN = config["COLS_FOR_SUM_DN"]
COLS_NO_NUMERICAS = config["COLS_NO_NUMERICAS"]
# Nombres de columnas de ventas
COLS_CNAL_SUBCNAL_SEG_F = config["COLS_CNAL_SUBCNAL_SEG_F"]
COLS_CNAL_SUBCNAL_SEG_F_FNIF = config["COLS_CNAL_SUBCNAL_SEG_F_FNIF"]

# Tipos de ventas
TIPO_VENTA_COMP = config["TIPO_VENTA_COMP"]
TIPO_VENTA_CADENAS = config["TIPO_VENTA_CADENAS"]

# Nombres de columnas de ventas
VENTAS = config["VENTAS"]
VENTA_CADENAS = config["VENTA_CADENAS"]
VENTA_COMP = config["VENTA_COMP"]

# Otros
AGENTE_COMERCIAL = config["AGENTE_COMERCIAL"]
FILLNA = config["FILLNA"]
DROP = config["DROP"]
ASTYPE = config["ASTYPE"]
CANAL_DIST = config["CANAL_DIST"]
COLS_NECESARIAS_VENTAS_M = config["COLS_NECESARIAS_VENTAS_M"]
COLS_NECESARIAS_VENTAS_SA = config["COLS_NECESARIAS_VENTAS_SA"]
COLS_SEC_SUBC_F = config["COLS_SEC_SUBC_F"]
COL_GRANDES_CADENAS = config["COL_GRANDES_CADENAS"]
SECTOR = config["SECTOR"]
SUBCANAL_SEGMENTO_2 = config["SUBCANAL_SEGMENTO_2"]
COLS_NUMERICAS = config["COLS_NUMERICAS"]
# Devoluciones
DEVS_MALAS = config["DEVS_MALAS"]
DEVS_SA = config["DEVS_SA"]
DEVS_PARAMETRO = config["DEVS_PARAMETRO"]
DEVS_MALAS_COL_FINAL = config["DEVS_MALAS_COL_FINAL"]
DEVS_COL_FINAL_MA = config["DEVS_COL_FINAL_MA"]
DEVS_COL_FINAL_SA = config["DEVS_COL_FINAL_SA"]

# Columnas concatenadas
CONCATENADA_CAD = config["CONCATENADA_CAD"]
CONCATENADA_COMP = config["CONCATENADA_COMP"]
CONCATENADA_GB_VN = config["CONCATENADA_GB_VN"]
# Columnas para calcular previo CxS
# Ventas
OFICINA_VENTAS = config["OFICINA_VENTAS"]
OFICINA_VENTAS_AGRUP = config["OFICINA_VENTAS_AGRUP"]
VENTA_EFECTIVA = config["VENTA_EFECTIVA"]
VENTA_NETAS_CN = config["VENTA_NETAS_CN"]
VENTA_NETAS_GRUPO = config["VENTA_NETAS_GRUPO"]
VENTA_EFECTIVA_GRUPO = config["VENTA_EFECTIVA_GRUPO"]
VENTA_EFECTIVA_FINAL= config["VENTA_EFECTIVA_FINAL"]
GASTO_PROM_COMER = config["GASTO_PROM_COMER"]

# Descuentos
DESCUENTOS = config["DESCUENTOS"]
DESCUENTOS_NG = config["DESCUENTOS_NG"]
DESCUENTOS_CN = config["DESCUENTOS_CN"]
DEPURACION_DCTOS = config["DEPURACION_DCTOS"]
DESCUENTOS_GRUPO = config["DESCUENTOS_GRUPO"]

#Reemplazos en consultas. 
REEMPLAZOS = config["REEMPLAZOS"]

#Diccionario remplazar regiones.
DICT_REGIONES = config["DICT_REGIONES"] 