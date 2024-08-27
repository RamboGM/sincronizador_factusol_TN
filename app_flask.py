from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv

app = Flask(__name__)

# Cargar variables de entorno
load_dotenv()

@app.route('/')
def home():
    return "API de Sincronización con Tienda Nube Activa"

# Ruta para manejar la instalación desde Tienda Nube
@app.route('/install', methods=['GET'])
def install():
    tienda_code = request.args.get('code')
    # Aquí manejarías la lógica de intercambio del código por el access_token con Tienda Nube
    return jsonify({"message": "Aplicación instalada correctamente"})

# Ruta para manejar webhooks
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    # Aquí manejarías los eventos recibidos desde Tienda Nube
    return jsonify({"message": "Webhook recibido"}), 200

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')

