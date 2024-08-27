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

def obtener_ruta_base():
    if getattr(sys, 'frozen', False):  # Verifica si está empaquetado con PyInstaller
        ruta_base = sys._MEIPASS  # Carpeta temporal donde se extraen los archivos
    else:
        ruta_base = os.path.dirname(os.path.abspath(__file__))  # Carpeta actual en modo de desarrollo
    return ruta_base

# Configuración de logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.StreamHandler(sys.stdout)
])

# Cargar variables de entorno desde el archivo .env
dotenv_path = os.path.join(obtener_ruta_base(), '.env')
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
            logger.info(message)
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
        wait_time = rate_reset / 1000.0
        logging.info(f"Rate limit alcanzado. Esperando {wait_time:.2f} segundos para continuar...")
        time.sleep(wait_time)

def obtener_productos_existentes():
    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    productos_existentes = []
    pagina = 1
    max_paginas = 50

    while True:
        params = {'page': pagina, 'per_page': 200}
        response = requests.get(api_url, headers=headers, params=params)
        manejar_rate_limit(response.headers)

        if response.status_code == 200:
            data = response.json()
            logging.debug(f"Productos obtenidos en la página {pagina}: {len(data)} productos")
            if not data:
                break
            productos_existentes.extend(data)

            link_header = response.headers.get('Link', '')
            if 'rel="next"' not in link_header or pagina >= max_paginas:
                break

            pagina += 1
        else:
            logging.error(f"Error al obtener productos: {response.status_code} {response.text}")
            break

    return productos_existentes

def normalizar_sku(sku):
    return sku.strip().upper()

def variantes_iguales(var_existente, var_nuevo):
    def safe_float(value):
        try:
            return float(value) if value is not None else 0.0
        except ValueError:
            return 0.0

    return (
        normalizar_sku(var_existente.get("sku")) == normalizar_sku(var_nuevo.get("sku")) and
        safe_float(var_existente.get("price")) == safe_float(var_nuevo.get("price")) and
        int(var_existente.get("stock") or 0) == int(var_nuevo.get("stock") or 0) and
        safe_float(var_existente.get("cost", 0)) == safe_float(var_nuevo.get("cost", 0))  # Compara también el costo
    )


def productos_iguales(prod_existente, prod_nuevo):
    nombre_existente = prod_existente.get("name", {}).get("es", "").strip().lower()
    nombre_nuevo = prod_nuevo.get("name", "").strip().lower()

    if nombre_existente != nombre_nuevo:
        logging.debug(f"Diferencia en nombre: '{nombre_existente}' vs '{nombre_nuevo}'")
        return False

    variantes_existente = prod_existente.get("variants", [])
    variantes_nuevo = prod_nuevo.get("variants", [])

    if len(variantes_existente) != len(variantes_nuevo):
        logging.debug(f"Diferencia en cantidad de variantes: {len(variantes_existente)} vs {len(variantes_nuevo)}")
        return False

    for var_nuevo in variantes_nuevo:
        match_found = False
        for var_existente in variantes_existente:
            if variantes_iguales(var_existente, var_nuevo):
                match_found = True
                break
        if not match_found:
            logging.debug(f"No se encontró coincidencia para la variante: {var_nuevo}")
            return False

    logging.debug(f"Producto {nombre_nuevo} es igual al existente.")
    return True

def actualizar_variantes(producto_id, variantes_nuevas, variantes_existentes):
    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    for variante_nueva in variantes_nuevas:
        variante_id = None
        for variante_existente in variantes_existentes:
            if normalizar_sku(variante_existente.get("sku")) == normalizar_sku(variante_nueva.get("sku")):
                variante_id = variante_existente.get("id")
                break
        
        if variante_id:
            url = f"{api_url}/{producto_id}/variants/{variante_id}"
            response = requests.put(url, headers=headers, json=variante_nueva)
            manejar_rate_limit(response.headers)

            if response.status_code == 200:
                logging.info(f"Variante {variante_id} del producto {producto_id} actualizada correctamente.")
            else:
                logging.error(f"Error al actualizar variante {variante_id} del producto {producto_id}: {response.status_code} {response.text}")
        else:
            logging.error(f"No se pudo encontrar una variante con SKU {variante_nueva.get('sku')} en el producto {producto_id}")

def actualizar_producto(producto_id, producto_data, variantes_existentes):
    global productos_actualizados

    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    producto_data_sin_variantes = {k: v for k, v in producto_data.items() if k != "variants"}
    url = f"{api_url}/{producto_id}"
    response = requests.put(url, headers=headers, json=producto_data_sin_variantes)
    manejar_rate_limit(response.headers)

    if response.status_code == 200:
        logging.info(f"Producto {producto_id} actualizado correctamente.")
        productos_actualizados += 1
        actualizar_variantes(producto_id, producto_data.get("variants", []), variantes_existentes)
    else:
        logging.error(f"Error al actualizar producto {producto_id}: {response.status_code} {response.text}")

def crear_producto(producto_data, log_func=None):
    global productos_creados

    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    response = requests.post(api_url, headers=headers, json=producto_data)
    manejar_rate_limit(response.headers)

    if response.status_code == 201:
        # Solo si la creación es exitosa incrementamos el contador
        if log_func:
            log_func("Producto creado correctamente.")
        productos_creados += 1  # Incrementa solo si el producto se crea con éxito
    else:
        # Controla cualquier otro tipo de error
        try:
            error_message = response.json()
        except json.JSONDecodeError:
            error_message = {"error": "No se pudo decodificar la respuesta del servidor."}

        if response.status_code == 422:
            if log_func:
                log_func(f"Error 422: Producto no creado debido a un problema en los datos (posiblemente stock negativo).")
        else:
            if log_func:
                log_func(f"Error {response.status_code}: {response.text}")

    return response.status_code

def eliminar_producto(producto_id):
    global productos_eliminados

    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    url = f"{api_url}/{producto_id}"
    response = requests.delete(url, headers=headers)
    manejar_rate_limit(response.headers)

    if response.status_code in [200, 204]:
        logging.info(f"Producto {producto_id} eliminado correctamente.")
        productos_eliminados += 1
    else:
        logging.error(f"Error al eliminar producto {producto_id}: {response.status_code} {response.text}")

def procesar_csv_a_json(csv_files):
    productos = []
    data = {}

    for csv_file in csv_files:
        with open(csv_file, newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=';')
            data[os.path.basename(csv_file)] = list(reader)
    
    for row in data.get("F_ART.csv", []):
        # Filtrar los productos según el valor de SUWART
        if row.get("SUWART") != "1":
            continue  # Si SUWART no es 1, se ignora este producto

        producto = {
            "name": row["DESART"],
            "sku": row["CODART"],
            "published": True,
            "requires_shipping": True,
            "stock_management": True,
            "variants": [],
            "attributes": []
        }
        
        arc_row = next((arc for arc in data.get("F_ARC.csv", []) if arc["ARTARC"] == row["CODART"]), None)
        
        if arc_row and ("CE1ARC" in arc_row and arc_row["CE1ARC"]):
            variante = {
                "sku": row["CODART"],
                "price": None,
                "stock": None,
                "barcode": row.get("EANART", ""),
                "cost": row.get("PCOART", None),
                "values": []
            }
            
            if arc_row["CE1ARC"]:
                producto["attributes"].append({"name": "Talle", "values": [arc_row["CE1ARC"]]})
                variante["values"].append(arc_row["CE1ARC"])
            if "CE2ARC" in arc_row and arc_row["CE2ARC"]:
                producto["attributes"].append({"name": "Color", "values": [arc_row["CE2ARC"]]})

            for lt_row in data.get("F_LTA.csv", []):
                if lt_row["ARTLTA"] == row["CODART"]:
                    variante["price"] = lt_row.get("PRELTA")
                    break

            for st_row in data.get("F_STO.csv", []):
                if st_row["ARTSTO"] == row["CODART"]:
                    variante["stock"] = int(float(st_row.get("DISSTO", 0)))
                    break

            producto["variants"].append(variante)
        else:
            variante_simple = {
                "sku": row["CODART"],
                "price": None,
                "stock": None,
                "barcode": row.get("EANART", ""),
                "cost": row.get("PCOART", None),
                "values": []
            }
            
            for lt_row in data.get("F_LTA.csv", []):
                if lt_row["ARTLTA"] == row["CODART"]:
                    variante_simple["price"] = lt_row.get("PRELTA")
                    break

            for st_row in data.get("F_STO.csv", []):
                if st_row["ARTSTO"] == row["CODART"]:
                    variante_simple["stock"] = int(float(st_row.get("DISSTO", 0)))
                    break

            producto["variants"].append(variante_simple)
            
        productos.append(producto)
    
    return productos


def sincronizar_productos(productos_nuevos, log_func=None, progress_bar=None, stop_event=None):
    global productos_creados, productos_actualizados, productos_eliminados
    productos_creados = 0
    productos_actualizados = 0
    productos_eliminados = 0

    productos_existentes = obtener_productos_existentes()

    productos_existentes_dict = {}
    productos_nuevos_dict = {}
    total_productos_procesados = 0  # Asegurarse de inicializar el contador correctamente
    total_productos = len(productos_nuevos)

    # Crear un diccionario con los productos existentes para búsqueda rápida por SKU
    for prod in productos_existentes:
        if "variants" in prod:
            for variant in prod["variants"]:
                sku = normalizar_sku(variant.get("sku"))
                if sku:
                    productos_existentes_dict[sku] = prod

    # Sincronización de productos
    for producto in productos_nuevos:
        if stop_event and stop_event.is_set():  # Verificación de cancelación
            log_func("Sincronización cancelada.")
            return  # Salir inmediatamente si se solicita la cancelación

        total_productos_procesados += 1  # Incrementar el contador al procesar un producto

        if "variants" in producto:
            for variant in producto["variants"]:
                sku = normalizar_sku(variant.get("sku"))
                if not sku:
                    log_func(f"Variante sin SKU en el JSON nuevo.")
                    continue

                if sku in productos_nuevos_dict:
                    continue  # Evitar procesar la misma variante dos veces

                productos_nuevos_dict[sku] = producto

                if sku in productos_existentes_dict:
                    producto_existente = productos_existentes_dict[sku]
                    log_func(f"Comparando producto existente con SKU: {sku}")
                    if productos_iguales(producto_existente, producto):
                        log_func(f"El producto SKU: {sku} ya está actualizado.")
                    else:
                        if stop_event and stop_event.is_set():  # Verificación de cancelación
                            log_func("Sincronización cancelada.")
                            return  # Salir inmediatamente si se solicita la cancelación
                        log_func(f"Actualizando producto SKU: {sku}")
                        if not producto_existente.get('sincronizado', False):  # Asegurarse de que no se actualice dos veces
                            actualizar_producto(producto_existente["id"], producto, producto_existente["variants"])
                            producto_existente['sincronizado'] = True  # Marcar como sincronizado
                else:
                    if stop_event and stop_event.is_set():  # Verificación de cancelación
                        log_func("Sincronización cancelada.")
                        return  # Salir inmediatamente si se solicita la cancelación
                    log_func(f"Creando nuevo producto SKU: {sku}")
                    if not producto.get('sincronizado', False):  # Asegurarse de que no se cree dos veces
                        status_code = crear_producto(producto)
                        if status_code == 201:  # Solo incrementa si se creó correctamente
                            producto['sincronizado'] = True  # Marcar como sincronizado

    # Verificación de productos a eliminar
    productos_eliminados_list = []
    for sku, producto_existente in productos_existentes_dict.items():
        if stop_event and stop_event.is_set():  # Verificación de cancelación
            log_func("Sincronización cancelada.")
            return  # Salir inmediatamente si se solicita la cancelación
        if sku not in productos_nuevos_dict:
            log_func(f"Eliminando producto con SKU: {sku} que ya no está en Base de Datos.")
            if not producto_existente.get('sincronizado', False):  # Asegurarse de que no se elimine dos veces
                eliminar_producto(producto_existente["id"])
                productos_eliminados_list.append(sku)
                producto_existente['sincronizado'] = True  # Marcar como sincronizado

    # Forzar que los logs se muestren correctamente
    log_func(f"\n--- Resumen de Sincronización ---")
    log_func(f"Productos creados: {productos_creados}")
    log_func(f"Productos actualizados: {productos_actualizados}")
    log_func(f"Productos eliminados: {productos_eliminados}")
    log_func(f"Total productos procesados: {total_productos_procesados}")  # Mostrar el total correctamente
    log_func(f"---------------------------------\n")

    log_func("Sincronización manual completada.")









