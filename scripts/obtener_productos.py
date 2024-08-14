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

def guardar_productos_json(productos, archivo_salida):
    with open(archivo_salida, 'w', encoding='utf-8') as f:
        json.dump(productos, f, ensure_ascii=False, indent=4)
    logging.info(f"Productos existentes guardados en {archivo_salida}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("No se proporcionó el nombre del archivo JSON de salida.")
        sys.exit(1)

    archivo_salida = sys.argv[1]
    ruta_archivo = os.path.join(os.getcwd(), archivo_salida)  # Guardar en la carpeta raíz del proyecto
    productos_existentes = obtener_productos_existentes()
    guardar_productos_json(productos_existentes, ruta_archivo)

