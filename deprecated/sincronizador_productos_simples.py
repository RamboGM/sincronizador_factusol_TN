import os
import pyodbc
import pandas as pd
import sys
import logging
import requests
import time
import json
import csv
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.StreamHandler(sys.stdout)
])

# Cargar variables de entorno desde el archivo .env
dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(dotenv_path)

# Obtener el token de acceso y el ID de usuario
access_token = os.getenv("ACCESS_TOKEN")
user_id = os.getenv("USER_ID")

# Establecer la URL base de la API
api_url = f"https://api.tiendanube.com/v1/{user_id}/products"

# Contadores globales
productos_creados = 0
productos_actualizados = 0
productos_eliminados = 0

def exportar_a_csv(access_file_path, csv_directory, send_to_gui=None):
    logger = logging.getLogger()

    # Comprobar que las rutas no están vacías
    if not access_file_path or not csv_directory:
        logger.error("La configuración de las rutas no está completa.")
        return

    # Crear el directorio CSV si no existe
    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)
        logger.info(f"Directorio {csv_directory} creado.")
        if send_to_gui:
            send_to_gui(f"Directorio {csv_directory} creado.")

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
            if send_to_gui:
                send_to_gui(message)
        
        except pyodbc.Error as e:
            error_message = f"Error al conectar con la base de datos: {e}"
            logger.error(error_message)
            if send_to_gui:
                send_to_gui(error_message)
        except Exception as e:
            error_message = f"Error al exportar la tabla {table_name}: {e}"
            logger.error(error_message)
            if send_to_gui:
                send_to_gui(error_message)
        
        finally:
            if 'conn' in locals():
                conn.close()

    # Exportar cada tabla a un archivo CSV en paralelo
    with ThreadPoolExecutor() as executor:
        executor.map(export_table_to_csv, tables)

def manejar_rate_limit(headers):
    rate_remaining = int(headers.get('x-rate-limit-remaining', 0))
    rate_reset = int(headers.get('x-rate-limit-reset', 0))
    if rate_remaining < 5:
        wait_time = max(rate_reset / 1000.0, 1)  # Espera al menos 1 segundo si no hay información de reset
        logging.info(f"Rate limit alcanzado. Esperando {wait_time:.2f} segundos para continuar...")
        time.sleep(wait_time)

def normalizar_sku(sku):
    if isinstance(sku, dict):
        logging.error(f"SKU esperado como cadena, pero se recibió un diccionario: {sku}")
        return ""
    elif sku is None:
        logging.error("SKU es None, se esperaba una cadena.")
        return ""
    else:
        return str(sku).strip().upper()

def procesar_csv_a_json(csv_files):
    productos = []
    data = {}

    # Leer los CSV y almacenarlos en un diccionario
    for csv_file in csv_files:
        with open(csv_file, newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=';')
            data[os.path.basename(csv_file)] = list(reader)
    
    # Procesar los productos de la tabla F_ART
    for row in data.get("F_ART.csv", []):
        # Filtrar los productos según el valor de SUWART
        if row.get("SUWART") != "1":
            continue  # Si SUWART no es 1, se ignora este producto

        # Crear la estructura básica del producto
        producto = {
            "name": {"es": row["DESART"]},
            "sku": row["CODART"],
            "published": True,
            "requires_shipping": True,
            "stock_management": True,
            "variants": []  # Variants se utilizará para enviar datos de SKU, precio, stock y costo
        }

        # Crear la estructura de la variante principal para el producto simple
        variante = {
            "sku": row["CODART"],  # El SKU del producto
            "price": float(row.get("PCOART", 0)),  # Asignar el precio desde F_ART o 0 si no está definido
            "stock": 0,  # Inicializar el stock en 0, se ajustará más adelante
            "cost": float(row.get("PCOART", 0)),  # Usar el mismo campo para el costo o ajustarlo si hay un campo específico
            "barcode": row.get("EANART", "")  # Asignar código de barras si existe
        }

        # Obtener stock para el producto simple desde la tabla F_STO
        for st_row in data.get("F_STO.csv", []):
            if st_row.get("ARTSTO") == row["CODART"]:
                stock_value = int(float(st_row.get("DISSTO", 0)))  # Convertir a entero
                variante["stock"] = max(0, stock_value)  # Asegurar que el stock no sea negativo
                break  # Detener el bucle después de encontrar el stock

        # Añadir la variante al producto
        producto["variants"].append(variante)

        # Añadir el producto procesado a la lista de productos
        productos.append(producto)

    return productos

def productos_iguales(prod_existente, prod_nuevo):
    # Compara los nombres de los productos
    nombre_existente = prod_existente.get("name", {}).get("es", "").strip().lower()
    nombre_nuevo = prod_nuevo.get("name", {}).get("es", "").strip().lower()

    if nombre_existente != nombre_nuevo:
        return False

    # Compara el estado de publicación
    if prod_existente.get("published") != prod_nuevo.get("published"):
        return False

    # Compara las variantes (SKU, precio, stock, costo)
    variantes_existentes = prod_existente.get("variants", [])
    variantes_nuevas = prod_nuevo.get("variants", [])

    if len(variantes_existentes) != len(variantes_nuevas):
        return False

    for var_existente, var_nueva in zip(variantes_existentes, variantes_nuevas):
        sku_existente = normalizar_sku(var_existente.get("sku", ""))
        sku_nuevo = normalizar_sku(var_nueva.get("sku", ""))

        if sku_existente != sku_nuevo:
            return False

        if float(var_existente.get("price", 0)) != float(var_nueva.get("price", 0)):
            return False

        if int(var_existente.get("stock", 0)) != int(var_nueva.get("stock", 0)):
            return False

        if float(var_existente.get("cost", 0)) != float(var_nueva.get("cost", 0)):
            return False

    # Si todas las comparaciones son iguales, los productos son iguales
    return True

def procesar_productos_simples(productos_simples, productos_existentes_dict, log_func=logging.warning, progress_bar=None, stop_event=None):
    """
    Procesa la lista de productos simples. Esta función incluiría la lógica de
    sincronización, creación o actualización de productos simples en la tienda.
    """
    if log_func is None:
        log_func = logging.warning

    logging.info("Iniciando la sincronización de productos simples.")

    # Sincronizar cada producto simple
    for producto in productos_simples:
        sku = normalizar_sku(producto.get("sku", ""))
        producto_existente = productos_existentes_dict.get(sku)

        if producto_existente:
            # Compara y actualiza el producto si es necesario
            if not productos_iguales(producto_existente, producto):
                logging.info(f"Actualizando producto SKU: {sku}")
                actualizar_producto(producto_existente["id"], producto, producto_existente.get("variants", []))
        else:
            logging.info(f"Creando nuevo producto SKU: {sku}")
            crear_producto(producto)

def actualizar_variantes(producto_id, variantes_nuevas):
    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    # Obtener variantes existentes del producto en Tienda Nube
    variantes_existentes = obtener_variantes_existentes(producto_id)

    # Crear un diccionario para variantes existentes basado en SKU y valores de variación
    variantes_existentes_dict = {
        (normalizar_sku(var.get("sku")), tuple(sorted(val.get("es") for val in var.get("values", []) if val.get("es")))): var
        for var in variantes_existentes
    }

    variantes_actualizadas = False

    for variante_nueva in variantes_nuevas:
        sku_normalizado = normalizar_sku(variante_nueva.get("sku"))
        valores_variacion = tuple(sorted(val.get("es") for val in variante_nueva.get("values", []) if val.get("es")))

        # Verificar si la variante ya existe en Tienda Nube
        key = (sku_normalizado, valores_variacion)
        if key in variantes_existentes_dict:
            variante_existente = variantes_existentes_dict[key]

            # Verificar si la variante necesita actualización comparando todos los campos relevantes
            if not variantes_iguales(variante_existente, variante_nueva):
                variante_id = variante_existente.get("id")
                url = f"{api_url}/{producto_id}/variants/{variante_id}"
                
                # Enviar la solicitud de actualización para la variante
                response = requests.put(url, headers=headers, json=variante_nueva)
                manejar_rate_limit(response.headers)

                if response.status_code == 200:
                    logging.info(f"Variante {variante_id} del producto {producto_id} actualizada correctamente.")
                    variantes_actualizadas = True  # Marcar que al menos una variante se actualizó
                else:
                    logging.error(f"Error al actualizar variante {variante_id} del producto {producto_id}: {response.status_code} {response.text}")
        else:
            # Si no existe la variante en Tienda Nube, crearla
            logging.info(f"Creando nueva variante {variante_nueva['sku']} para el producto {producto_id} con valores {valores_variacion}...")
            if not crear_variante(producto_id, variante_nueva):
                logging.warning(f"Se omitió la creación de la variante con SKU {sku_normalizado} porque ya existe.")

    return variantes_actualizadas  # Devuelve True si se actualizó alguna variante

def actualizar_producto(producto_id, producto_data, variantes_existentes):
    global productos_actualizados

    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    # Excluir variantes del producto para la actualización inicial
    producto_data_sin_variantes = {k: v for k, v in producto_data.items() if k != "variants"}
    url = f"{api_url}/{producto_id}"
    
    # Realizar la actualización del producto sin variantes
    response = requests.put(url, headers=headers, json=producto_data_sin_variantes)
    manejar_rate_limit(response.headers)

    if response.status_code == 200:
        logging.info(f"Producto {producto_id} actualizado correctamente.")
        productos_actualizados += 1  # Incrementar el contador aquí
        
        # Actualizar variantes después de actualizar el producto
        actualizar_variantes(producto_id, producto_data.get("variants", []))
    else:
        logging.error(f"Error al actualizar producto {producto_id}: {response.status_code} {response.text}")

def crear_producto(producto_data, log_func=None):
    global productos_creados

    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    logging.debug(f"Enviando JSON para crear producto: {json.dumps(producto_data, indent=4)}")

    response = requests.post(api_url, headers=headers, json=producto_data)
    manejar_rate_limit(response.headers)

    if response.status_code == 201:
        if log_func:
            log_func("Producto creado correctamente.")
        productos_creados += 1
    else:
        try:
            error_message = response.json()
            logging.error(f"Error creando producto: {response.status_code} - {error_message}")
        except json.JSONDecodeError:
            error_message = {"error": "No se pudo decodificar la respuesta del servidor."}
            logging.error(f"Error creando producto: {response.status_code} - {response.text}")

        if log_func:
            log_func(f"Error {response.status_code}: {error_message}")

    return response.status_code

def eliminar_producto(producto_id):
    global productos_eliminados

    headers = {
        'Authentication': f'bearer {access_token}',  
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    url = f"{api_url}/{producto_id}"
    logging.debug(f"Eliminando producto con ID: {producto_id} - URL: {url}")

    response = requests.delete(url, headers=headers)
    manejar_rate_limit(response.headers)

    if response.status_code in [200, 204]:
        logging.info(f"Producto {producto_id} eliminado correctamente.")
        productos_eliminados += 1
    else:
        logging.error(f"Error al eliminar producto {producto_id}: {response.status_code} {response.text}")
        if response.status_code == 404:
            logging.warning(f"Producto {producto_id} no encontrado al intentar eliminar.")
        elif response.status_code == 403:
            logging.error(f"Permisos insuficientes para eliminar producto {producto_id}.")
        else:
            logging.error(f"Error desconocido al eliminar producto {producto_id}.")
