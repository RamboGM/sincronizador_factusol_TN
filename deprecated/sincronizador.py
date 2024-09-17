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

def obtener_variantes_existentes(producto_id):
    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    url = f"{api_url}/{producto_id}/variants"
    response = requests.get(url, headers=headers)
    manejar_rate_limit(response.headers)

    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Error al obtener variantes del producto {producto_id}: {response.status_code} {response.text}")
        return []

def variantes_iguales(var_existente, var_nuevo):
    def safe_float(value):
        try:
            return float(value) if value is not None else 0.0
        except ValueError:
            return 0.0

    sku_existente = str(var_existente.get("sku", "")).strip().upper()
    sku_nuevo = str(var_nuevo.get("sku", "")).strip().upper()

    return (
        sku_existente == sku_nuevo and
        safe_float(var_existente.get("price")) == safe_float(var_nuevo.get("price")) and
        int(var_existente.get("stock") or 0) == int(var_nuevo.get("stock") or 0) and
        safe_float(var_existente.get("cost", 0)) == safe_float(var_nuevo.get("cost", 0))
    )

def productos_iguales(prod_existente, prod_nuevo):
    # Compara nombres de productos asegurando que sean cadenas y estén normalizados
    nombre_existente = prod_existente.get("name", {}).get("es", "").strip().lower()
    nombre_nuevo = prod_nuevo.get("name", {}).get("es", "").strip().lower()

    if nombre_existente != nombre_nuevo:
        logging.debug(f"Diferencia en nombre: '{nombre_existente}' vs '{nombre_nuevo}'")
        return False

    # Comparación de variantes
    variantes_existente = prod_existente.get("variants", [])
    variantes_nuevo = prod_nuevo.get("variants", [])

    if len(variantes_existente) != len(variantes_nuevo):
        logging.debug(f"Diferencia en cantidad de variantes: {len(variantes_existente)} vs {len(variantes_nuevo)}")
        return False

    for var_nuevo in variantes_nuevo:
        # Verifica que cada variante nueva tenga una coincidencia exacta en las variantes existentes
        if not any(variantes_iguales(var_existente, var_nuevo) for var_existente in variantes_existente):
            logging.debug(f"No se encontró coincidencia para la variante: {var_nuevo}")
            return False

    # Si todas las verificaciones pasan, los productos se consideran iguales
    logging.debug(f"Producto '{nombre_nuevo}' es igual al existente.")
    return True

def actualizar_variantes(producto_id, variantes_nuevas):
    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    variantes_existentes = obtener_variantes_existentes(producto_id)
    variantes_existentes_dict = {
        (normalizar_sku(var.get("sku")), tuple(sorted(val.get("es") for val in var.get("values", []) if val.get("es")))): var
        for var in variantes_existentes
    }

    for variante_nueva in variantes_nuevas:
        sku_normalizado = normalizar_sku(variante_nueva.get("sku"))
        valores_variacion = tuple(sorted(val.get("es") for val in variante_nueva.get("values", []) if val.get("es")))

        if not valores_variacion:
            logging.warning(f"Variante con SKU {sku_normalizado} no tiene valores de variación válidos, se omitirá.")
            continue

        key = (sku_normalizado, valores_variacion)
        if key in variantes_existentes_dict:
            variante_existente = variantes_existentes_dict[key]
            if not variantes_iguales(variante_existente, variante_nueva):
                variante_id = variante_existente.get("id")
                url = f"{api_url}/{producto_id}/variants/{variante_id}"
                response = requests.put(url, headers=headers, json=variante_nueva)
                manejar_rate_limit(response.headers)

                if response.status_code == 200:
                    logging.info(f"Variante {variante_id} del producto {producto_id} actualizada correctamente.")
                else:
                    logging.error(f"Error al actualizar variante {variante_id} del producto {producto_id}: {response.status_code} {response.text}")
            else:
                logging.info(f"Variante {sku_normalizado} con valores {valores_variacion} ya está actualizada y no necesita cambios.")
        else:
            logging.info(f"Creando nueva variante {variante_nueva['sku']} para el producto {producto_id} con valores {valores_variacion}...")
            if not crear_variante(producto_id, variante_nueva):
                logging.warning(f"Se omitió la creación de la variante con SKU {sku_normalizado} porque ya existe.")

def crear_variante(producto_id, variante_data):
    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    url = f"{api_url}/{producto_id}/variants"
    response = requests.post(url, headers=headers, json=variante_data)
    manejar_rate_limit(response.headers)

    if response.status_code == 201:
        logging.info(f"Variante para producto {producto_id} creada correctamente.")
        return True
    else:
        logging.error(f"Error al crear variante para producto {producto_id}: {response.status_code} {response.text}")
        if response.status_code == 422 and "Variants cannot be repeated" in response.text:
            logging.warning(f"Variante con SKU {variante_data['sku']} ya existe para el producto {producto_id}. No se creará nuevamente.")
        return False

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
        actualizar_variantes(producto_id, producto_data.get("variants", []))
    else:
        logging.error(f"Error al actualizar producto {producto_id}: {response.status_code} {response.text}")

def crear_producto(producto_data, log_func=None, max_retries=3):
    """
    Crea un producto en Tienda Nube y maneja errores de servidor.
    """
    global productos_creados

    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    # Intentar enviar la solicitud hasta un máximo de intentos (retries)
    for intento in range(max_retries):
        response = requests.post(api_url, headers=headers, json=producto_data)
        manejar_rate_limit(response.headers)

        if response.status_code == 201:
            if log_func:
                log_func("Producto creado correctamente.")
            productos_creados += 1
            return response.status_code

        elif response.status_code == 500:
            # Registrar el error 500 y reintentar después de una espera
            logging.error(f"Error creando producto: {response.status_code} - {response.text}")
            if log_func:
                log_func(f"Error {response.status_code}: {response.text}")
            if intento < max_retries - 1:
                wait_time = (intento + 1) * 2  # Espera exponencial antes del siguiente intento
                logging.info(f"Reintentando en {wait_time} segundos...")
                time.sleep(wait_time)
            else:
                # Después del último intento, registrar el error final
                logging.error(f"No se pudo crear el producto después de {max_retries} intentos.")
                if log_func:
                    log_func(f"Error final al crear producto: {response.status_code} - {response.text}")
                return response.status_code
        else:
            # Manejo de otros errores
            try:
                error_message = response.json()
                logging.error(f"Error creando producto: {response.status_code} - {json.dumps(error_message)}")
            except json.JSONDecodeError:
                error_message = {"error": "No se pudo decodificar la respuesta del servidor."}
                logging.error(f"Error creando producto: {response.status_code} - {response.text}")


            if log_func:
                log_func(f"Error {response.status_code}: {error_message}")
            return response.status_code

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
            "variants": [],
            "attributes": []
        }

        # Inicializar los atributos y variantes
        atributos = {"Talle": set()}  # Usamos un conjunto para evitar duplicados
        variantes = []

        # Buscar todas las variantes del producto en F_ARC
        for arc_row in data.get("F_ARC.csv", []):
            if arc_row["ARTARC"] == row["CODART"]:
                # Descartar variantes si no tienen valores en CE1ARC o CE2ARC
                if not arc_row["CE1ARC"] and not arc_row.get("CE2ARC"):
                    logging.warning(f"Variante con SKU {row['CODART']} no tiene valores en CE1ARC o CE2ARC, se omitirá.")
                    continue

                variante = {
                    "sku": row['CODART'],  # Usar el SKU original de la base de datos
                    "price": None,
                    "stock": None,
                    "barcode": row.get("EANART", ""),
                    "cost": row.get("PCOART", None),
                    "values": []
                }

                # Agregar valores de atributos de variantes
                if arc_row["CE1ARC"]:
                    atributos["Talle"].add(arc_row["CE1ARC"])
                    variante["values"].append({"es": arc_row["CE1ARC"]})

                if arc_row.get("CE2ARC"):
                    if "Color" not in atributos:
                        atributos["Color"] = set()
                    atributos["Color"].add(arc_row["CE2ARC"])
                    variante["values"].append(arc_row["CE2ARC"])

                # Obtener precio para la variante de la tabla F_LTC
                for lt_row in data.get("F_LTC.csv", []):
                    if lt_row["ARTLTC"] == row["CODART"] and lt_row["CE1LTC"] == arc_row["CE1ARC"]:
                        variante["price"] = lt_row.get("PRELTC")
                        break

                # Obtener stock para la variante de la tabla F_STC
                stock_asignado = False
                for st_row in data.get("F_STC.csv", []):
                    if st_row.get("ARTSTC") == row["CODART"] and st_row.get("CE1STC") and st_row["CE1STC"].strip() == arc_row.get("CE1ARC", "").strip():
                        # Asignar el stock correspondiente a cada variante
                        stock_value = int(float(st_row.get("DISSTC", 0)))
                        if stock_value < 0:
                            variante["stock"] = 0  # Establecer stock en 0 si es negativo
                        else:
                            variante["stock"] = stock_value  # Asignar el stock tal como es si es positivo o cero
                        stock_asignado = True
                        break

                if not stock_asignado:
                    # Si no se encontró ningún stock, asumir 0
                    variante["stock"] = 0

                # Agregar la variante a la lista de variantes
                variantes.append(variante)

        # Convertir los atributos en la estructura correcta para el JSON
        for nombre_atributo, valores in atributos.items():
            producto["attributes"].append({
                "name": nombre_atributo,
                "values": list(valores)  # Convertir el set en lista
            })

        # Asignar todas las variantes procesadas al producto
        producto["variants"] = variantes

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
    total_productos_procesados = 0
    total_productos = len(productos_nuevos)

    # Crear diccionarios para búsqueda rápida por SKU
    for prod in productos_existentes:
        if "variants" in prod:
            for variant in prod["variants"]:
                sku = normalizar_sku(variant.get("sku"))
                if sku:
                    productos_existentes_dict[sku] = prod

    # Sincronización de productos
    for producto in productos_nuevos:
        if stop_event and stop_event.is_set():
            log_func("Sincronización cancelada.")
            return

        total_productos_procesados += 1

        if "variants" in producto:
            for variant in producto["variants"]:
                sku = normalizar_sku(variant.get("sku"))
                if not sku:
                    log_func(f"Variante sin SKU en el JSON nuevo.")
                    continue

                if sku in productos_nuevos_dict:
                    continue

                productos_nuevos_dict[sku] = producto

                if sku in productos_existentes_dict:
                    producto_existente = productos_existentes_dict[sku]
                    log_func(f"Comparando producto existente con SKU: {sku}")
                    if productos_iguales(producto_existente, producto):
                        log_func(f"El producto SKU: {sku} ya está actualizado.")
                    else:
                        if stop_event and stop_event.is_set():
                            log_func("Sincronización cancelada.")
                            return
                        log_func(f"Actualizando producto SKU: {sku}")
                        if not producto_existente.get('sincronizado', False):
                            actualizar_producto(producto_existente["id"], producto, producto_existente["variants"])
                            producto_existente['sincronizado'] = True
                else:
                    if stop_event and stop_event.is_set():
                        log_func("Sincronización cancelada.")
                        return
                    log_func(f"Creando nuevo producto SKU: {sku}")
                    if not producto.get('sincronizado', False):
                        status_code = crear_producto(producto)
                        if status_code == 201:
                            producto['sincronizado'] = True
                            productos_creados += 1  # Incrementa solo si el producto se crea con éxito

    # Verificación de productos a eliminar
    productos_eliminados_list = []
    for sku, producto_existente in productos_existentes_dict.items():
        if stop_event and stop_event.is_set():
            log_func("Sincronización cancelada.")
            return
        if sku not in productos_nuevos_dict:
            log_func(f"Eliminando producto con SKU: {sku} que ya no está en Base de Datos.")
            if not producto_existente.get('sincronizado', False):
                eliminar_producto(producto_existente["id"])
                productos_eliminados_list.append(sku)
                producto_existente['sincronizado'] = True
                productos_eliminados += 1  # Incrementa solo si el producto se elimina con éxito

    # Log de resumen
    log_func(f"\n--- Resumen de Sincronización ---")
    log_func(f"Productos creados: {productos_creados}")
    log_func(f"Productos actualizados: {productos_actualizados}")
    log_func(f"Productos eliminados: {productos_eliminados}")
    log_func(f"Total productos procesados: {total_productos_procesados}")
    log_func(f"---------------------------------\n")

    log_func("Sincronización manual completada.")

