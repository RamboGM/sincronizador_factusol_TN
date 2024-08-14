import requests

def obtener_productos(token, user_id):
    url = f'https://api.tiendanube.com/v1/{user_id}/products'
    headers = {
        'Authentication': f'bearer {token}',
        'User-Agent': 'MyApp (name@email.com)'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print('Datos de productos:', data)
        return data
    else:
        print(f'Error: {response.status_code} - {response.text}')
        return None

# Reemplaza con tus valores reales
token = '60c33c9eda90dcac972518236a876a4a6e3f3919'
user_id = '3915392'

obtener_productos(token, user_id)
