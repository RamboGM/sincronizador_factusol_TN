import json
import os
import sys
import requests
from dotenv import load_dotenv
import time
import logging

# Configuración de logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.StreamHandler(sys.stdout)
])

# Cargar variables de entorno desde el archivo .env
dotenv_path = '.env'
load_dotenv(dotenv_path)

# Obtener el token de acceso y el ID de usuario
access_token = os.getenv("ACCESS_TOKEN")
user_id = os.getenv("USER_ID")

# Establecer la URL base de la API
api_url = f"https://api.tiendanube.com/v1/{user_id}/products"

# Contadores globales
productos_creados = 0
productos_actualizados = 0
productos_eliminados = 0  # Contador de productos eliminados

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
    max_paginas = 50  # Puedes ajustar esto según tus necesidades

    while True:
        params = {'page': pagina, 'per_page': 200}
        response = requests.get(api_url, headers=headers, params=params)
        manejar_rate_limit(response.headers)

        if response.status_code == 200:
            data = response.json()
            logging.debug(f"Productos obtenidos en la página {pagina}: {data}")
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
    """
    Compara dos variantes para determinar si son iguales.
    """
    return (
        normalizar_sku(var_existente.get("sku")) == normalizar_sku(var_nuevo.get("sku")) and
        float(var_existente.get("price")) == float(var_nuevo.get("price")) and
        int(var_existente.get("stock")) == int(var_nuevo.get("stock"))
    )

def productos_iguales(prod_existente, prod_nuevo):
    """
    Compara dos productos para determinar si son iguales. Recorre todas las variantes para comparar.
    """
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
        # Buscar la variante existente correspondiente por SKU
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
        productos_creados += 1
    else:
        logging.error(f"Error al crear producto: {response.status_code} {response.text}")

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

    if response.status_code in [200, 204]:  # Aceptamos 200 o 204 como éxito
        logging.info(f"Producto {producto_id} eliminado correctamente.")
        productos_eliminados += 1
    else:
        logging.error(f"Error al eliminar producto {producto_id}: {response.status_code} {response.text}")

def sincronizar_productos(json_file):
    with open(json_file, 'r', encoding='utf-8') as f:
        productos_nuevos = json.load(f)

    productos_existentes = obtener_productos_existentes()

    productos_existentes_dict = {}
    total_productos_procesados = 0  # Contador de productos procesados

    for prod in productos_existentes:
        if "variants" in prod:
            for variant in prod["variants"]:
                sku = normalizar_sku(variant.get("sku"))
                if sku:
                    productos_existentes_dict[sku] = prod
                else:
                    logging.warning(f"Variante sin SKU encontrada: {variant}")
        else:
            logging.warning(f"Producto sin variantes encontrado: {prod}")

    productos_nuevos_dict = {}
    for producto in productos_nuevos:
        total_productos_procesados += 1  # Incrementar el contador

        if "variants" in producto:
            for variant in producto["variants"]:
                sku = normalizar_sku(variant.get("sku"))
                if not sku:
                    logging.warning(f"Variante sin SKU en el JSON nuevo: {variant}")
                    continue

                productos_nuevos_dict[sku] = producto

                if sku in productos_existentes_dict:
                    producto_existente = productos_existentes_dict[sku]
                    logging.debug(f"Comparando producto existente con SKU: {sku}")
                    if productos_iguales(producto_existente, producto):
                        logging.info(f"El producto SKU: {sku} ya está actualizado.")
                    else:
                        logging.info(f"Actualizando producto SKU: {sku}")
                        actualizar_producto(producto_existente["id"], producto, producto_existente["variants"])
                else:
                    logging.info(f"Creando nuevo producto SKU: {sku}")
                    crear_producto(producto)

    # Eliminar productos que existen en la tienda pero no en el JSON nuevo
    productos_eliminados_list = []
    for sku, producto_existente in productos_existentes_dict.items():
        if sku not in productos_nuevos_dict:
            logging.info(f"Eliminando producto con SKU: {sku} que ya no está en el JSON.")
            eliminar_producto(producto_existente["id"])
            productos_eliminados_list.append(sku)

    # Imprimir resumen al finalizar
    logging.info(f"Sincronización completada. Productos creados: {productos_creados}, Productos actualizados: {productos_actualizados}, Productos eliminados: {len(productos_eliminados_list)}. Productos procesados: {total_productos_procesados}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("No se proporcionó el archivo JSON a sincronizar.")
        sys.exit(1)

    json_file = sys.argv[1]
    sincronizar_productos(json_file)

