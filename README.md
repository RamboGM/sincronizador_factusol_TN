# Sincronizador Tienda Nube - Factusol

Este es un proyecto que permite la sincronización de productos entre Factusol y Tienda Nube mediante el procesamiento de archivos CSV generados desde la base de datos de Factusol. El objetivo principal de la herramienta es facilitar la carga, actualización y eliminación de productos entre ambas plataformas.

## Características

- Sincronización de productos (crear, actualizar, eliminar) entre Factusol y Tienda Nube.
- Interfaz gráfica de usuario (GUI) para seleccionar rutas de archivos y configurar la sincronización.
- Soporte para configurar la sincronización automática a una hora determinada.
- Almacena configuraciones persistentes en un archivo `config.txt` para que no sea necesario reconfigurar en cada uso.
- Logs para seguimiento de las acciones realizadas y los errores encontrados.

## Requisitos previos

Antes de instalar y ejecutar el proyecto, asegúrate de tener instalado:

- Python 3.7 o superior
- Pip (el administrador de paquetes de Python)
- [Tienda Nube API](https://tiendanube.github.io/api-documentation/intro) y las credenciales correspondientes.

## Instalación

1. **Clona el repositorio:**

   ```bash
   git clone https://github.com/tu-usuario/sincronizador-tienda-nube-factusol.git
   cd sincronizador-tienda-nube-factusol
Crea un entorno virtual (opcional pero recomendado):

python -m venv venv
source venv/bin/activate  # En Linux/macOS
.\\venv\\Scripts\\activate  # En Windows
Instala las dependencias:

pip install -r requirements.txt
Configura el entorno:

Crea un archivo .env en la carpeta scripts/ con las siguientes variables:

API_KEY_TIENDA_NUBE=<tu_api_key>
STORE_ID=<tu_store_id>
Ejecuta la aplicación:


python main.py
Uso
Configuración inicial:
Al iniciar la aplicación, selecciona la ruta de la base de datos de Factusol (.mdb o .accdb).
Selecciona el directorio donde deseas guardar los archivos CSV.
Configura si deseas gestionar los precios y/o el stock de los productos.
Guarda la configuración. Esta se almacenará en el archivo config.txt para futuras ejecuciones.
Sincronización manual:
Para sincronizar los productos manualmente, haz clic en el botón "Sincronizar Ahora".
Se generarán los archivos CSV necesarios y se sincronizarán los productos con Tienda Nube.
Sincronización automática:
Puedes programar la sincronización automática seleccionando una hora y activando la opción de sincronización automática.
La sincronización ocurrirá a la hora configurada.

Estructura del proyecto:
sincronizador-tienda-nube-factusol/
│
├── scripts/                  # Contiene los scripts de sincronización y .env
│   ├── sincronizador.py       # Lógica de sincronización entre Factusol y Tienda Nube
│   └── config.txt             # Archivo de configuración guardada del programa
│
├── main.py                   # Archivo principal que ejecuta la interfaz gráfica
├── README.md                 # Este archivo
├── requirements.txt          # Lista de dependencias del proyecto
└── venv/                     # Entorno virtual (opcional)

Dependencias
Las principales dependencias del proyecto incluyen:

tkinter para la interfaz gráfica
configparser para la gestión de configuraciones
apscheduler para la programación de sincronizaciones automáticas
python-dotenv para manejar las variables de entorno
logging para el registro de logs de actividad
requests para las peticiones a la API de Tienda Nube
Consulta el archivo requirements.txt para ver la lista completa de dependencias.

Contribuciones
¡Las contribuciones son bienvenidas! Si deseas mejorar el proyecto, sigue los siguientes pasos:

Haz un fork del proyecto.
Crea una nueva rama (git checkout -b feature-nueva-funcionalidad).
Realiza tus cambios y haz commit (git commit -am 'Añadir nueva funcionalidad').
Haz push a la rama (git push origin feature-nueva-funcionalidad).
Abre un Pull Request y describe tus cambios.
Licencia
Este proyecto está licenciado bajo la MIT License - consulta el archivo LICENSE para más detalles.
