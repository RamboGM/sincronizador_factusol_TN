# scripts/sincronizador.py

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

# Inicialización
dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(dotenv_path)

access_token = os.getenv("ACCESS_TOKEN")
user_id = os.getenv("USER_ID")
api_url = f"https://api.tiendanube.com/v1/{user_id}/products"

productos_creados = 0
productos_actualizados = 0
productos_eliminados = 0

# Configuración de logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.StreamHandler(sys.stdout)
])

def obtener_headers():
    return {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

def exportar_a_csv(access_file_path, csv_directory, send_to_gui=None):
    logger = logging.getLogger()

    if not access_file_path or not csv_directory:
        logger.error("La configuración de las rutas no está completa.")
        return

    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)
        logger.info(f"Directorio {csv_directory} creado.")
        if send_to_gui:
            send_to_gui(f"Directorio {csv_directory} creado.")

    tables = ["F_ART", "F_ARC", "F_STO", "F_STC", "F_LTA", "F_LTC", "F_ALM", "F_TAR", "F_FAM", "F_SEC"]

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

    with ThreadPoolExecutor() as executor:
        executor.map(export_table_to_csv, tables)

def manejar_rate_limit(headers):
    rate_remaining = int(headers.get('x-rate-limit-remaining', 0))
    rate_reset = int(headers.get('x-rate-limit-reset', 0))
    
    if rate_remaining < 5:
        wait_time = max(rate_reset / 1000.0, 1)
        logging.info(f"Rate limit alcanzado. Esperando {wait_time:.2f} segundos para continuar...")
        time.sleep(wait_time)
    elif rate_remaining < 10:
        logging.info("Cerca del límite de tasa, reduciendo la frecuencia de las solicitudes...")
        time.sleep(1)

def normalizar_sku(sku):
    return sku.strip().upper() if sku else ""

def obtener_productos_existentes():
    headers = obtener_headers()
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
                break  # No hay más productos, terminamos el bucle

            for producto in data:
                producto_id = producto.get('id')
                variantes = obtener_variantes_existentes(producto_id)
                producto['variants'] = variantes
                productos_existentes.append(producto)

            # Verificar si hay un link para la siguiente página
            link_header = response.headers.get('Link', '')
            if 'rel="next"' not in link_header or pagina >= max_paginas:
                logging.debug(f"No hay más páginas. Última página obtenida: {pagina}")
                break  # No hay más páginas, salir del loop

            pagina += 1
        else:
            logging.error(f"Error al obtener productos: {response.status_code} {response.text}")
            break

    return productos_existentes

def obtener_variantes_existentes(producto_id, reintentos=3):
    headers = obtener_headers()
    url_variants = f"{api_url}/{producto_id}/variants"
    variantes_existentes = []

    for intento in range(reintentos):
        response = requests.get(url_variants, headers=headers)
        manejar_rate_limit(response.headers)

        if response.status_code == 200:
            variantes_existentes = response.json()
            logging.debug(f"Variantes obtenidas para el producto {producto_id}: {len(variantes_existentes)} variantes")
            break  # Salir del ciclo al obtener correctamente las variantes

        elif response.status_code == 500:
            logging.warning(f"Error al obtener variantes para el producto {producto_id} (Intento {intento + 1} de {reintentos}): {response.status_code} {response.text}")
            time.sleep(2 ** intento)  # Exponencial backoff

        elif response.status_code == 404:
            logging.error(f"El producto {producto_id} no tiene variantes o no se encuentra. Error 404.")
            break  # No tiene sentido seguir reintentando si es un 404

        else:
            logging.error(f"Error al obtener variantes para el producto {producto_id}: {response.status_code} {response.text}")
            break  # Otros errores, salir del ciclo

    return variantes_existentes

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
        safe_float(var_existente.get("cost", 0)) == safe_float(var_nuevo.get("cost", 0))
    )

def productos_iguales(prod_existente, prod_nuevo):
    nombre_existente = " ".join(prod_existente.get("name", {}).get("es", "").split()).lower()
    nombre_nuevo = " ".join(prod_nuevo.get("name", {}).get("es", "").split()).lower()

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

def actualizar_producto(producto_id, producto_data, variantes_existentes, gestionar_precio=True, gestionar_stock=True):
    global productos_actualizados

    headers = obtener_headers()

    # Crear una copia del producto_data, solo trabajaremos con las variantes
    producto_data_sin_variantes = {
        "variants": producto_data.get("variants", [])
    }

    # Mapear las variantes existentes por SKU para encontrar el ID correcto
    variantes_existentes_dict = {normalizar_sku(v.get('sku')): v for v in variantes_existentes}

    # Si no se deben gestionar el precio o stock, eliminarlos de las variantes
    for variante in producto_data_sin_variantes["variants"]:
        sku_normalizado = normalizar_sku(variante.get('sku'))
        variante_existente = variantes_existentes_dict.get(sku_normalizado)

        if variante_existente:
            variante['id'] = variante_existente.get('id')
        else:
            logging.warning(f"No se encontró variante existente para SKU: {sku_normalizado}. Verifica que el SKU esté correcto.")

        if not gestionar_precio:
            variante.pop("price", None)
        if not gestionar_stock:
            variante.pop("stock", None)

    logging.debug(f"Actualizando variantes para producto {producto_id}. {len(producto_data_sin_variantes['variants'])} variantes serán actualizadas.")

    for variante in producto_data_sin_variantes["variants"]:
        variante_id = variante.get("id")
        logging.debug(f"Procesando variante con ID: {variante_id}")

        if variante_id:
            url_variante = f"{api_url}/{producto_id}/variants/{variante_id}"
            response_variante = requests.put(url_variante, headers=headers, json=variante)
            manejar_rate_limit(response_variante.headers)

            if response_variante.status_code == 200:
                logging.info(f"Variante {variante_id} del producto {producto_id} actualizada correctamente.")
                productos_actualizados += 1
            else:
                logging.error(f"Error al actualizar variante {variante_id}: {response_variante.status_code} {response_variante.text}")
        else:
            logging.warning(f"No se encontró el ID de la variante para el producto {producto_id}. Saltando la actualización de esta variante.")

def actualizar_variantes(producto_id, variantes_nuevas):
    headers = obtener_headers()
    variantes_existentes = obtener_variantes_existentes(producto_id)
    
    variantes_existentes_dict = {
        (normalizar_sku(var.get("sku")), tuple(sorted(val.get("es") for val in var.get("values", []) if val.get("es")))): var
        for var in variantes_existentes
    }

    for variante_nueva in variantes_nuevas:
        sku_normalizado = normalizar_sku(variante_nueva.get("sku"))
        valores_variacion = tuple(sorted(val.get("es") for val in variante_nueva.get("values", []) if val.get("es")))

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
    headers = obtener_headers()
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

def crear_producto(producto_data, log_func=None):
    global productos_creados

    headers = obtener_headers()

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

    headers = obtener_headers()

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
        if row.get("SUWART") != "1":
            continue

        descripcion_producto = row.get("DEWART", "")

        producto = {
            "name": {"es": row["DESART"]},
            "sku": row["CODART"],
            "description": { "es": descripcion_producto },  # Descripción del producto obtenida de DEWART
            "published": True,
            "requires_shipping": True,
            "stock_management": True,
            "variants": [],
            "attributes": []
        }

        variantes_encontradas = [arc_row for arc_row in data.get("F_ARC.csv", []) if arc_row["ARTARC"] == row["CODART"]]
        if variantes_encontradas:
            atributos_set = set()

            for arc_row in variantes_encontradas:
                if not arc_row.get("CE1ARC") and not arc_row.get("CE2ARC"):
                    continue

                variante = {
                    "sku": row["CODART"],
                    "price": None,
                    "stock": None,
                    "barcode": row.get("EANART", ""),
                    "cost": row.get("PCOART", None),
                    "values": []
                }

                if arc_row["CE1ARC"]:
                    atributos_set.add("Talle")
                    variante["values"].append({"es": arc_row["CE1ARC"]})

                if "CE2ARC" in arc_row and arc_row["CE2ARC"]:
                    atributos_set.add("Color")
                    variante["values"].append({"es": arc_row["CE2ARC"]})

                for lt_row in data.get("F_LTC.csv", []):
                    if lt_row["ARTLTC"] == row["CODART"] and lt_row["CE1LTC"] == arc_row["CE1ARC"]:
                        variante["price"] = lt_row.get("PRELTC")
                        break

                stock_asignado = False
                for st_row in data.get("F_STC.csv", []):
                    if not st_row.get("CE1STC"):
                        continue

                    if st_row["ARTSTC"] == row["CODART"] and st_row["CE1STC"] == arc_row["CE1ARC"]:
                        stock_value = int(float(st_row.get("DISSTC", 0)))
                        variante["stock"] = max(stock_value, 0)
                        stock_asignado = True
                        break

                if not stock_asignado:
                    variante["stock"] = 0

                producto["variants"].append(variante)

            variantes_unicas = []
            combinaciones = set()
            for variante in producto["variants"]:
                valores_tuple = tuple(sorted([v["es"] for v in variante["values"]]))
                if valores_tuple not in combinaciones:
                    combinaciones.add(valores_tuple)
                    variantes_unicas.append(variante)

            producto["variants"] = variantes_unicas

            for nombre in atributos_set:
                producto["attributes"].append({"es": nombre})

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
                    stock_value = int(float(st_row.get("DISSTO", 0)))
                    variante_simple["stock"] = max(stock_value, 0)
                    break

            producto["variants"].append(variante_simple)

        productos.append(producto)

    return productos

def ocultar_producto(producto_id):
    global productos_ocultados  # Cambiamos a productos_ocultados, no productos_eliminados

    headers = obtener_headers()
    url = f"{api_url}/{producto_id}"
    data = {
        "published": False
    }

    response = requests.put(url, headers=headers, json=data)
    manejar_rate_limit(response.headers)

    if response.status_code == 200:
        logging.info(f"Producto {producto_id} ocultado correctamente.")
        productos_ocultados += 1  # Incrementamos el contador correcto
    else:
        logging.error(f"Error al ocultar producto {producto_id}: {response.status_code} {response.text}")

def sincronizar_productos(productos_nuevos, log_func=None, stop_event=None, gestionar_precio=True, gestionar_stock=True, crear_productos=True, ocultar_no_existentes=False):

    global productos_creados, productos_actualizados, productos_eliminados, productos_ocultados
    productos_creados = 0
    productos_actualizados = 0
    productos_eliminados = 0
    productos_ocultados = 0

    productos_existentes = obtener_productos_existentes()

    productos_existentes_dict = {normalizar_sku(variant.get("sku")): prod for prod in productos_existentes for variant in prod.get("variants", [])}
    productos_nuevos_dict = {normalizar_sku(prod.get("sku")): prod for prod in productos_nuevos}

    total_productos_procesados = 0
    total_productos = len(productos_nuevos)

    for producto_nuevo in productos_nuevos:
        if stop_event and stop_event.is_set():
            log_func("Sincronización cancelada.")
            return

        total_productos_procesados += 1
        sku = normalizar_sku(producto_nuevo.get("sku", ""))

        if not sku:
            log_func(f"Producto sin SKU, ignorado.")
            continue

        producto_existente = productos_existentes_dict.get(sku)

        if producto_existente:
            log_func(f"Comparando producto existente con SKU: {sku}")
            if productos_iguales(producto_existente, producto_nuevo):
                log_func(f"El producto SKU: {sku} ya está actualizado. Verificando variantes...")
                actualizar_variantes(producto_existente["id"], producto_nuevo.get("variants", []))
            else:
                log_func(f"Actualizando producto SKU: {sku}")
                actualizar_producto(producto_existente["id"], producto_nuevo, producto_existente.get("variants", []), gestionar_precio, gestionar_stock)
        else:
            if crear_productos:
                log_func(f"Creando nuevo producto SKU: {sku}")
                crear_producto(producto_nuevo)
            else:
                log_func(f"El producto SKU: {sku} no existe en Tienda Nube y la opción 'Crear Productos' está deshabilitada.")

    # Verificar los productos que ya no existen en Factusol
    for sku, producto_existente in productos_existentes_dict.items():
        if stop_event and stop_event.is_set():
            log_func("Sincronización cancelada.")
            return
        
        # Si el producto ya no existe en Factusol, verificar si debe ser ocultado o eliminado
        if sku not in productos_nuevos_dict:
            # Verificamos si el producto ya está oculto en la tienda
            if not producto_existente.get("published", True):
                log_func(f"El producto con SKU {sku} ya está oculto en la tienda, no se tomará ninguna acción.")
                continue  # Si ya está oculto, no hacemos nada

            # Si no está oculto y la opción es ocultar, lo ocultamos
            if ocultar_no_existentes:
                log_func(f"Ocultando producto con SKU: {sku} que ya no está en la base de datos.")
                ocultar_producto(producto_existente["id"])  # Oculta el producto
                productos_ocultados += 1  # Solo incrementa productos ocultados
            else:
                log_func(f"Eliminando producto con SKU: {sku} que ya no está en la base de datos.")
                eliminar_producto(producto_existente["id"])  # Elimina el producto
                productos_eliminados += 1  # Solo incrementa si realmente fue eliminado

    # Mostrar el resumen de sincronización
    log_func(f"\n--- Resumen de Sincronización ---")
    log_func(f"Productos creados: {productos_creados}")
    log_func(f"Productos actualizados: {productos_actualizados}")
    log_func(f"Productos eliminados: {productos_eliminados}")
    log_func(f"Productos ocultados en tienda: {productos_ocultados}")
    log_func(f"Total productos procesados: {total_productos_procesados}")
    log_func(f"---------------------------------\n")

    log_func("Sincronización manual completada.")





