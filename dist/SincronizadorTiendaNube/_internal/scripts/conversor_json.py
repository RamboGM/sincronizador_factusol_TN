import json
import csv
import os
import sys
import argparse
from dotenv import load_dotenv
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cargar variables de entorno desde el archivo .env
dotenv_path = '.env'
logging.info(f"Cargando archivo .env desde: {dotenv_path}")
load_dotenv(dotenv_path)

# Obtener el token de acceso y el ID de usuario desde el archivo .env
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
USER_ID = os.getenv("USER_ID")

# Verificar que las variables de entorno existan
if not ACCESS_TOKEN or not USER_ID:
    logging.error("Falta ACCESS_TOKEN o USER_ID en el archivo .env.")
    exit(1)

# Función para procesar los datos de múltiples CSV y generar un JSON para Tienda Nube
def procesar_csv_a_json(csv_files):
    productos = []
    data = {}

    # Leer cada archivo CSV y agrupar datos relevantes
    for csv_file in csv_files:
        with open(csv_file, newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=';')
            data[os.path.basename(csv_file)] = list(reader)
    
    # Ahora, combinamos los datos y generamos los productos
    for row in data.get("F_ART.csv", []):
        producto = {
            "name": row["DESART"],
            "sku": row["CODART"],  # SKU viene de la columna CODART en F_ART
            "published": True,
            "requires_shipping": True,
            "stock_management": True,
            "variants": [],
            "attributes": []
        }
        
        # Buscar variaciones en la tabla F_ARC usando CODART como referencia
        arc_row = next((arc for arc in data.get("F_ARC.csv", []) if arc["ARTARC"] == row["CODART"]), None)
        
        if arc_row and ("CE1ARC" in arc_row and arc_row["CE1ARC"]):
            # Producto con variantes
            variante = {
                "sku": row["CODART"],
                "price": None,
                "stock": None,
                "barcode": row.get("EANART", ""),
                "cost": row.get("PCOART", None),
                "values": []
            }
            
            # Asignar valores de variación y precio/costo/stock si las columnas existen
            if arc_row["CE1ARC"]:
                producto["attributes"].append({"name": "Talle", "values": [arc_row["CE1ARC"]]})
                variante["values"].append(arc_row["CE1ARC"])
            if "CE2ARC" in arc_row and arc_row["CE2ARC"]:
                producto["attributes"].append({"name": "Color", "values": [arc_row["CE2ARC"]]})

            # Encontrar precio, stock y costo desde otros CSV usando SKU
            for lt_row in data.get("F_LTA.csv", []):
                if lt_row["ARTLTA"] == row["CODART"]:  # Comparar usando SKU correcto
                    variante["price"] = lt_row.get("PRELTA")
                    break

            for st_row in data.get("F_STO.csv", []):
                if st_row["ARTSTO"] == row["CODART"]:  # Comparar usando SKU correcto
                    variante["stock"] = int(float(st_row.get("DISSTO", 0)))  # Convertir stock a entero
                    break

            producto["variants"].append(variante)
        else:
            # Producto simple: Asignar precio, stock y costo usando SKU
            variante_simple = {
                "sku": row["CODART"],
                "price": None,
                "stock": None,
                "barcode": row.get("EANART", ""),
                "cost": row.get("PCOART", None),
                "values": []
            }
            
            for lt_row in data.get("F_LTA.csv", []):
                if lt_row["ARTLTA"] == row["CODART"]:  # Comparar usando SKU correcto
                    variante_simple["price"] = lt_row.get("PRELTA")
                    break

            for st_row in data.get("F_STO.csv", []):
                if st_row["ARTSTO"] == row["CODART"]:  # Comparar usando SKU correcto
                    variante_simple["stock"] = int(float(st_row.get("DISSTO", 0)))  # Convertir stock a entero
                    break

            producto["variants"].append(variante_simple)
            
        productos.append(producto)
    
    # Guardar el JSON generado
    json_file = "productos.json"
    with open(json_file, 'w', encoding='utf-8') as jsonf:
        json.dump(productos, jsonf, ensure_ascii=False, indent=4)
    logging.info(f"Archivo JSON generado en: {json_file}")
    print(f"Archivo JSON generado en: {json_file}")

# Agregar manejo de argumentos para procesar varios CSV
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convierte múltiples CSV a un JSON para Tienda Nube.")
    parser.add_argument("csv_files", nargs='+', help="Rutas de los archivos CSV a procesar.")
    args = parser.parse_args()
    
    procesar_csv_a_json(args.csv_files)


















































