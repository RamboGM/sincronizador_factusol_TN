import requests
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener el token de acceso y el ID de usuario desde el archivo .env
access_token = os.getenv('ACCESS_TOKEN')
user_id = os.getenv('USER_ID')

# URL de la API para obtener las categorías
api_url = f'https://api.tiendanube.com/v1/{user_id}/categories'

# Encabezados
headers = {
    'Authentication': f'Bearer {access_token}',
    'User-Agent': 'Integrador Factusol 2 (info@tiendapocket.com)',
    'Content-Type': 'application/json'
}

# Hacer una solicitud GET para obtener las categorías
response = requests.get(api_url, headers=headers)

# Verificar el estado de la respuesta
if response.status_code == 200:
    print('Categorías obtenidas exitosamente.')
    categorias = response.json()
    for categoria in categorias:
        print(f"ID: {categoria['id']}, Nombre: {categoria['name']}")
else:
    print(f'Error al obtener las categorías: {response.status_code}')
    print(response.text)
