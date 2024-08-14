import os
import pyodbc
import pandas as pd
import sys
import logging
from concurrent.futures import ThreadPoolExecutor

# Configuración del logging para enviar los mensajes tanto a la consola como a la interfaz gráfica
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Supongamos que tenemos una función para enviar mensajes a la interfaz gráfica:
def send_to_gui(message):
    # Esta función debería estar implementada en el entorno de la interfaz gráfica
    # Para este ejemplo, simplemente usaremos un print, pero debería ser sustituido.
    print(message)

# Verificar que se han proporcionado los argumentos necesarios
if len(sys.argv) < 3:
    logger.error("La configuración de las rutas no está completa.")
    sys.exit(1)

# Obtener las rutas desde los argumentos de la línea de comandos
access_file_path = sys.argv[1]
csv_directory = sys.argv[2]

# Comprobar que las rutas no están vacías
if not access_file_path or not csv_directory:
    logger.error("La configuración de las rutas no está completa.")
    sys.exit(1)

# Crear el directorio CSV si no existe
if not os.path.exists(csv_directory):
    os.makedirs(csv_directory)
    logger.info(f"Directorio {csv_directory} creado.")

# Lista de tablas a exportar
tables = ["F_ART", "F_ARC", "F_STO", "F_STC", "F_LTA", "F_LTC", "F_ALM", "F_TAR", "F_FAM", "F_SEC"]

# Función para exportar datos de una tabla a un archivo CSV
def export_table_to_csv(table_name):
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        r"DBQ=" + access_file_path + ";"
    )
    try:
        conn = pyodbc.connect(conn_str)
        query = f"SELECT * FROM {table_name}"
        
        df = pd.read_sql(query, conn)
        csv_file_path = os.path.join(csv_directory, f"{table_name}.csv")
        
        df.to_csv(csv_file_path, sep=';', index=False, encoding='utf-8')
        message = f"Datos exportados de la tabla {table_name} a {csv_file_path}"
        logger.info(message)
        send_to_gui(message)
    
    except pyodbc.Error as e:
        error_message = f"Error al conectar con la base de datos: {e}"
        logger.error(error_message)
        send_to_gui(error_message)
    except Exception as e:
        error_message = f"Error al exportar la tabla {table_name}: {e}"
        logger.error(error_message)
        send_to_gui(error_message)
    
    finally:
        if 'conn' in locals():
            conn.close()

# Exportar cada tabla a un archivo CSV en paralelo
def export_all_tables():
    with ThreadPoolExecutor() as executor:
        executor.map(export_table_to_csv, tables)

# Llamada a la función
export_all_tables()








