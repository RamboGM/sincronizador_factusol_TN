import requests
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Obtener el token de acceso y el ID de usuario
access_token = os.getenv("ACCESS_TOKEN")
user_id = os.getenv("USER_ID")

def probar_conexion():
    if not access_token or not user_id:
        print("Error: Falta ACCESS_TOKEN o USER_ID en el archivo .env.")
        exit(1)

    api_url = f"https://api.tiendanube.com/v1/{user_id}/products"
    headers = {
        'Authentication': f'bearer {access_token}',
        'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            print("Conexión exitosa a Tienda Nube.")
        else:
            print(f"Error al conectarse a Tienda Nube: {response.status_code} {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error de conexión: {e}")

# Ejecutar la prueba de conexión
probar_conexion()




