# Importamos las librerias necesarias para la configuración.
import sys
from os import path

# Configuramos la ruta para traer las funciones.
current_dir = path.dirname(path.abspath(__file__))
parent_dir = path.dirname(current_dir)
new_dir = path.join(parent_dir, "Utils")
new_dir2 = osnew_dir = path.join(parent_dir, "Scripts")
sys.path.append(new_dir)


# Importamos la funcion especifica para medir el tiempo de ejecución del programa.
# Importamos el modulo de la configuración. 
import General_Functions as gf
# Agregamos el decorador para medir el tiempo de ejecución del programa.
@gf.registro_tiempo
def Proceso_CxS():
    # Importamos el modulo logger_funtions para llevar registro de funciones con logs
    import logger_functions
    # Importamos el programa a Ejecutar.
    import Exportar_sql
if __name__ == "__main__":
    Proceso_CxS()
    

