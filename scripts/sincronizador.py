import json
import csv
import os
import sys
import requests
from dotenv import load_dotenv
import time
import logging
import argparse

# Configuración de logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.StreamHandler(sys.stdout)
])

# Cargar variables de entorno desde el archivo .env
dotenv_path = r'C:\Users\Windows\Documents\sincronizador-factusol - copia\scripts\.env'
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
    return (
        normalizar_sku(var_existente.get("sku")) == normalizar_sku(var_nuevo.get("sku")) and
        float(var_existente.get("price")) == float(var_nuevo.get("price")) and
        int(var_existente.get("stock")) == int(var_nuevo.get("stock")) and
        float(var_existente.get("cost", 0)) == float(var_nuevo.get("cost", 0))  # Compara también el costo
    )

def productos_iguales(prod_existente, prod_nuevo):
    # Compara los nombres del producto
    nombre_existente = prod_existente.get("name", {}).get("es", "").strip().lower()
    nombre_nuevo = prod_nuevo.get("name", "").strip().lower()

    if nombre_existente != nombre_nuevo:
        logging.debug(f"Diferencia en nombre: '{nombre_existente}' vs '{nombre_nuevo}'")
        return False

    # Compara las variantes usando la función variantes_iguales
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

def crear_producto(producto_data):
    global productos_creados

    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    response = requests.post(api_url, headers=headers, json=producto_data)
    manejar_rate_limit(response.headers)

    if response.status_code == 201:
        logging.info("Producto creado correctamente.")
        productos_creados += 1  # Incrementa el contador solo si se crea con éxito.
    else:
        error_message = response.json()
        logging.error(f"Error al crear producto: {response.status_code} {response.text}")

        # Si el error es específico del stock negativo, puedes manejarlo de manera especial.
        if response.status_code == 422 and "variants[0].stock" in error_message:
            logging.warning(f"Producto no creado debido a stock negativo: {producto_data.get('sku')}")

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

def sincronizar_productos(productos_nuevos, log_func=None, progress_bar=None):
    global productos_creados, productos_actualizados, productos_eliminados
    productos_creados = 0
    productos_actualizados = 0
    productos_eliminados = 0

    productos_existentes = obtener_productos_existentes()
    productos_existentes_dict = {}
    total_productos_procesados = 0
    total_productos = len(productos_nuevos)

    for prod in productos_existentes:
        if "variants" in prod:
            for variant in prod["variants"]:
                sku = normalizar_sku(variant.get("sku"))
                if sku:
                    productos_existentes_dict[sku] = prod
                else:
                    if log_func:
                        log_func(f"Variante sin SKU encontrada.")
        else:
            if log_func:
                log_func(f"Producto sin variantes encontrado.")

    productos_nuevos_dict = {}
    for i, producto in enumerate(productos_nuevos):
        total_productos_procesados += 1

        if "variants" in producto:
            for variant in producto["variants"]:
                sku = normalizar_sku(variant.get("sku"))
                if not sku:
                    if log_func:
                        log_func(f"Variante sin SKU en el JSON nuevo.")
                    continue

                productos_nuevos_dict[sku] = producto

                if sku in productos_existentes_dict:
                    producto_existente = productos_existentes_dict[sku]
                    if log_func:
                        log_func(f"Comparando producto existente con SKU: {sku}")
                    if productos_iguales(producto_existente, producto):
                        if log_func:
                            log_func(f"El producto SKU: {sku} ya está actualizado.")
                    else:
                        if log_func:
                            log_func(f"Actualizando producto SKU: {sku}")
                        actualizar_producto(producto_existente["id"], producto, producto_existente["variants"])
                        productos_actualizados += 1
                else:
                    if log_func:
                        log_func(f"Creando nuevo producto SKU: {sku}")
                    # Verificamos el éxito de la creación antes de incrementar el contador
                    crear_producto(producto)

        # Actualizar barra de progreso
        if progress_bar:
            progress = (i + 1) / total_productos * 100
            progress_bar['value'] = progress

    productos_eliminados_list = []
    for sku, producto_existente in productos_existentes_dict.items():
        if sku not in productos_nuevos_dict:
            if log_func:
                log_func(f"Eliminando producto con SKU: {sku} que ya no está en el JSON.")
            eliminar_producto(producto_existente["id"])
            productos_eliminados_list.append(sku)
            productos_eliminados += 1

    # Asegurarse de que el resumen final se loguee correctamente
    if log_func:
        log_func(f"\n--- Resumen de Sincronización ---")
        log_func(f"Productos creados: {productos_creados}")
        log_func(f"Productos actualizados: {productos_actualizados}")
        log_func(f"Productos eliminados: {productos_eliminados}")
        log_func(f"Total productos procesados: {total_productos_procesados}")
        log_func(f"---------------------------------\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sincroniza productos de CSV a Tienda Nube.")
    parser.add_argument("csv_files", nargs='+', help="Rutas de los archivos CSV a procesar.")
    args = parser.parse_args()

    productos_nuevos = procesar_csv_a_json(args.csv_files)
    sincronizar_productos(productos_nuevos)

