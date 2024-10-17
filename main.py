import tkinter as tk
from tkinter import filedialog, messagebox, Toplevel
import shutil
from tkinter import ttk
from pathlib import Path
import threading
import os
import sys
import configparser
import webbrowser
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from scripts.sincronizador import procesar_csv_a_json, sincronizar_productos, exportar_a_csv
import logging

def obtener_ruta_base():
    return os.path.dirname(os.path.abspath(__file__))

def obtener_ruta_config():
    if os.name == 'nt':  # Windows
        carpeta_config = os.path.join(os.getenv('LOCALAPPDATA'), 'sincronizador_app')
    else:  # Linux, Mac
        carpeta_config = os.path.join(os.getenv('HOME'), '.config', 'sincronizador_app')

    # Crear la carpeta si no existe
    Path(carpeta_config).mkdir(parents=True, exist_ok=True)

    # Ruta completa del archivo config.txt
    return os.path.join(carpeta_config, 'config.txt')

config_path = os.path.join(obtener_ruta_base(), 'config.txt')

config_path = os.path.join(obtener_ruta_base(), 'scripts', 'config.txt')

def configurar_icono(root):
    icono_path = os.path.join(obtener_ruta_base(), "icon.ico")
    if os.path.exists(icono_path):
        root.iconbitmap(icono_path)
    else:
        logging.warning("El archivo de icono no se encontró en la ruta especificada.")

def mostrar_info(titulo, mensaje):
    top = Toplevel()
    top.title(titulo)
    top.geometry("300x150")
    top.resizable(False, False)
    label = ttk.Label(top, text=mensaje, wraplength=280, justify="left")
    label.pack(pady=10, padx=10)
    cerrar_boton = ttk.Button(top, text="Cerrar", command=top.destroy)
    cerrar_boton.pack(pady=10)

dotenv_path = os.path.join(obtener_ruta_base(), 'scripts', '.env')
load_dotenv(dotenv_path)

if not os.path.exists(dotenv_path):
    logging.error(f"No se encontró el archivo .env en: {dotenv_path}")
if not os.path.exists(config_path):
    logging.error(f"No se encontró el archivo config.txt en: {config_path}")

scheduler = BackgroundScheduler()
log_text = None
running_thread = None
stop_event = threading.Event()

global productos_creados, productos_actualizados, productos_eliminados
productos_creados = 0
productos_actualizados = 0
productos_eliminados = 0

class TextHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
        self.search_results = []
        self.current_match_index = 0

    def emit(self, record):
        try:
            msg = self.format(record)
            self.text_widget.config(state=tk.NORMAL)
            self.text_widget.insert(tk.END, msg + "\n")
            self.text_widget.see(tk.END)
            self.text_widget.config(state=tk.DISABLED)
        except Exception as e:
            print(f"Error al emitir log: {e}")

    def buscar_en_logs(self, search_term):
        self.text_widget.tag_remove("current_highlight", "1.0", tk.END)
        self.search_results.clear()

        if search_term:
            start_pos = "1.0"
            while True:
                start_pos = self.text_widget.search(search_term, start_pos, stopindex=tk.END)
                if not start_pos:
                    break
                end_pos = f"{start_pos}+{len(search_term)}c"
                self.search_results.append((start_pos, end_pos))
                start_pos = end_pos

            if self.search_results:
                self.current_match_index = 0
                self.mostrar_coincidencia()

    def mostrar_coincidencia(self):
        if self.search_results:
            self.text_widget.tag_remove("current_highlight", "1.0", tk.END)
            current_pos, end_pos = self.search_results[self.current_match_index]
            self.text_widget.tag_add("current_highlight", current_pos, end_pos)
            self.text_widget.tag_config("current_highlight", background="orange", foreground="black")
            self.text_widget.see(current_pos)

    def siguiente_coincidencia(self):
        if self.search_results:
            self.current_match_index = (self.current_match_index + 1) % len(self.search_results)
            self.mostrar_coincidencia()

    def anterior_coincidencia(self):
        if self.search_results:
            self.current_match_index = (self.current_match_index - 1) % len(self.search_results)
            self.mostrar_coincidencia()

def limpiar_estado():
    global productos_creados, productos_actualizados, productos_eliminados
    productos_creados = 0
    productos_actualizados = 0
    productos_eliminados = 0
    if scheduler.running:
        scheduler.shutdown()
    if running_thread and running_thread.is_alive():
        stop_event.set()

def leer_configuracion():
    config = configparser.ConfigParser()
    config_path = obtener_ruta_config()

    # Crear archivo de configuración si no existe y copiar valores por defecto
    if not os.path.exists(config_path):
        # Copiar config.txt predeterminado desde la carpeta empaquetada
        ruta_predeterminada = os.path.join(obtener_ruta_base(), 'scripts', 'config.txt')
        shutil.copy(ruta_predeterminada, config_path)
        logging.info(f"Archivo config.txt copiado a {config_path}")

    # Leer el archivo de configuración
    config.read(config_path)
    return config

def guardar_configuracion(db_path, csv_path, gestionar_precio, gestionar_stock, crear_productos, accion_no_existentes, hora_sincronizacion):
    config = leer_configuracion()
    config_path = obtener_ruta_config()

    # Guardar todas las configuraciones como strings
    config['DEFAULT'] = {
        'db_path': db_path.get(),
        'csv_path': csv_path.get(),
        'hora_sincronizacion': hora_sincronizacion.get(),
        'gestionar_precio': str(gestionar_precio.get()),  # Convertimos el valor booleano a string
        'gestionar_stock': str(gestionar_stock.get()),    # Convertimos el valor booleano a string
        'crear_productos': str(crear_productos.get()),    # Convertimos el valor booleano a string
        'accion_no_existentes': accion_no_existentes.get()
    }

    with open(config_path, 'w') as configfile:
        config.write(configfile)

    messagebox.showinfo("Guardado", "Configuración guardada correctamente.")
    logging.info(f"Configuración guardada en {config_path} correctamente.")

def inicializar_configuracion(db_path, csv_path, gestionar_precio, gestionar_stock, crear_productos, accion_no_existentes, hora_sincronizacion):
    config = leer_configuracion()

    # Cargar las rutas y otros valores
    db_path.set(config['DEFAULT'].get('db_path', ''))
    csv_path.set(config['DEFAULT'].get('csv_path', ''))
    hora_sincronizacion.set(config['DEFAULT'].get('hora_sincronizacion', ''))

    # Revisamos qué valores se están leyendo de config.txt
    precio = config['DEFAULT'].get('gestionar_precio', 'False')  # Valor como string
    stock = config['DEFAULT'].get('gestionar_stock', 'False')    # Valor como string
    productos = config['DEFAULT'].get('crear_productos', 'False') # Valor como string

    # Convertimos a booleanos y los asignamos
    gestionar_precio.set(precio == 'True')  # Cargar como booleano
    gestionar_stock.set(stock == 'True')    # Cargar como booleano
    crear_productos.set(productos == 'True')# Cargar como booleano

    # Cargar valor de ocultar/eliminar productos
    accion_no_existentes.set(config['DEFAULT'].get('accion_no_existentes', 'Ocultar'))

def obtener_hora_sincronizacion_guardada():
    config = leer_configuracion()
    return config['DEFAULT'].get('hora_sincronizacion', '')

def main():
    global log_text
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    # Crear la ventana principal
    root = tk.Tk()
    root.title("Sincronizador Tienda Nube")
    configurar_icono(root)

    root.state("zoomed")

    montserrat = ("Montserrat", 9)
    title_font = ("Montserrat", 14, "bold")
    title_label = tk.Label(root, text="Sincronizador Factusol | Tienda Nube", font=title_font, fg="#01304f")
    title_label.grid(row=0, column=0, pady=10, columnspan=4)

    # Declaración de variables
    db_path = tk.StringVar(value="")
    csv_path = tk.StringVar(value="")
    hora_sincronizacion = tk.StringVar(value="")
    gestionar_precio = tk.BooleanVar()
    gestionar_stock = tk.BooleanVar()   
    crear_productos = tk.BooleanVar() 
    accion_no_existentes = tk.StringVar()

    inicializar_configuracion(db_path, csv_path, gestionar_precio, gestionar_stock, crear_productos, accion_no_existentes, hora_sincronizacion)

    # Funciones para seleccionar archivos y directorios
    def seleccionar_db():
        path = filedialog.askopenfilename(
            title="Seleccionar archivo de base de datos de Factusol",
            filetypes=[("Access Database", "*.mdb;*.accdb"), ("All Files", "*.*")]
        )
        if path:
            db_path.set(path)

    def seleccionar_directorio_csv():
        path = filedialog.askdirectory(title="Seleccionar directorio para guardar archivos CSV")
        if path:
            csv_path.set(path)

    def advertencia_productos_duplicados(skus_duplicados):
        """
        Muestra una advertencia de productos duplicados en la interfaz gráfica.
        """
        mensaje = "Se han encontrado los siguientes SKUs duplicados:\n\n"
        mensaje += "\n".join(skus_duplicados)
        mensaje += "\n\nPor favor, corrige estos SKUs antes de continuar."

        # Ventana emergente
        top = Toplevel()
        top.title("Advertencia de SKUs Duplicados")
        top.geometry("400x200")
        top.resizable(False, False)
        label = ttk.Label(top, text=mensaje, wraplength=380, justify="left")
        label.pack(pady=10, padx=10)
        cerrar_boton = ttk.Button(top, text="Cerrar", command=top.destroy)
        cerrar_boton.pack(pady=10)

    def verificar_skus_duplicados(productos_nube):
        """
        Verifica si hay productos con SKUs duplicados en Tienda Nube.
        Retorna una lista de SKUs duplicados.
        """
        sku_dict = {}
        skus_duplicados = []

        for producto in productos_nube:
            sku = producto['sku']
            if sku in sku_dict:
                skus_duplicados.append(sku)
            else:
                sku_dict[sku] = producto

        return skus_duplicados


    def sincronizacion_manual():
        global running_thread
        try:
            logging.info("Iniciando sincronización manual...")
            if stop_event.is_set():
                logging.info("Sincronización cancelada antes de comenzar.")
                return

            exportar_a_csv(db_path.get(), csv_path.get(), send_to_gui=log)

            if stop_event.is_set():
                logging.info("Sincronización cancelada después de exportar CSV.")
                return

            csv_files = [
                os.path.join(csv_path.get(), "F_ART.csv"),
                os.path.join(csv_path.get(), "F_LTA.csv"),
                os.path.join(csv_path.get(), "F_STO.csv"),
                os.path.join(csv_path.get(), "F_ARC.csv"),
                os.path.join(csv_path.get(), "F_STC.csv"),
                os.path.join(csv_path.get(), "F_LTC.csv")
            ]

            productos_nuevos = procesar_csv_a_json(csv_files)

            # Verificar si hay productos con SKUs duplicados
            skus = [producto['sku'] for producto in productos_nuevos]
            skus_duplicados = [sku for sku in set(skus) if skus.count(sku) > 1]

            if skus_duplicados:
                # Registrar en el logging
                mensaje_duplicados = f"Se encontraron SKUs duplicados: {', '.join(skus_duplicados)}. Por favor, corrígelos antes de continuar."
                logging.warning(mensaje_duplicados)
                log(mensaje_duplicados)

                # Mostrar la advertencia en la interfaz gráfica
                advertencia_productos_duplicados(skus_duplicados)

                # Detener el proceso ya que no podemos continuar con SKUs duplicados
                return

            # Pasar los valores de los checkboxes a la función de sincronización
            sincronizar_productos(
                productos_nuevos,
                log_func=log,
                stop_event=stop_event,
                gestionar_precio=gestionar_precio.get(),
                gestionar_stock=gestionar_stock.get(),
                crear_productos=crear_productos.get(),
                accion_no_existentes=accion_no_existentes.get()  # Usar valor de string
            )

        except Exception as e:
            logging.info(f"Error en sincronización manual: {e}")
        finally:
            running_thread = None
            stop_event.clear()


    def log(message):
        if log_text:
            log_text.config(state=tk.NORMAL)
            log_text.insert(tk.END, message + "\n")
            log_text.see(tk.END)
            log_text.config(state=tk.DISABLED)
        print(f"{message}")

    def iniciar_sincronizacion():
        global running_thread
        if running_thread is None:
            running_thread = threading.Thread(target=sincronizacion_manual)
            running_thread.start()
        else:
            log("Sincronización ya en ejecución.")

    def cancelar_sincronizacion():
        global running_thread
        if running_thread and running_thread.is_alive():
            log("Solicitando cancelación de la sincronización...")
            stop_event.set()
        else:
            log("No hay sincronización en curso.")

    def activar_sincronizacion_automatica():
        hora = hora_sincronizacion.get()
        if hora:
            try:
                hh, mm = hora.split(":")
                scheduler.add_job(sincronizacion_manual, CronTrigger(hour=hh, minute=mm))
                scheduler.start()
                log(f"Sincronización automática programada a las {hora}.")
            except Exception as e:
                log(f"Error al programar la sincronización automática: {e}")
        else:
            log("Por favor, seleccione una hora válida (HH:MM) para la sincronización automática.")

    def cancelar_sincronizacion_automatica():
        scheduler.remove_all_jobs()
        log("Sincronización automática cancelada.")

    root.minsize(800, 1000)

    main_frame = ttk.Frame(root, padding="10")
    main_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    db_button = ttk.Button(main_frame, text="Seleccionar Base de Datos", command=seleccionar_db, style='TButton')
    db_button.grid(row=0, column=0, pady=5, sticky="ew")

    csv_button = ttk.Button(main_frame, text="Seleccionar Directorio CSV", command=seleccionar_directorio_csv, style='TButton')
    csv_button.grid(row=0, column=1, pady=5, sticky="ew")

    db_label_frame = ttk.LabelFrame(main_frame, text="Ruta de la Base de Datos")
    db_label_frame.grid(row=1, column=0, pady=5, padx=10, columnspan=2, sticky="ew")
    db_label_frame.grid_columnconfigure(0, weight=1)
    db_label = ttk.Label(db_label_frame, textvariable=db_path, font=montserrat, background="white", relief="solid", padding=5, anchor='w')
    db_label.grid(row=0, column=0, sticky="ew")

    csv_label_frame = ttk.LabelFrame(main_frame, text="Carpeta para archivos CSV")
    csv_label_frame.grid(row=2, column=0, pady=5, padx=10, columnspan=2, sticky="ew")
    csv_label_frame.grid_columnconfigure(0, weight=1)
    csv_label = ttk.Label(csv_label_frame, textvariable=csv_path, font=montserrat, background="white", relief="solid", padding=5, anchor='w')
    csv_label.grid(row=0, column=0, sticky="ew")

    # Añadir el texto antes de los checkboxes
    ttk.Label(main_frame, text="Por favor, seleccione que tareas desea realizar:", font=("Montserrat", 9, "bold")).grid(row=4, column=0, columnspan=2, pady=(10, 0), sticky="ew")

    checkbox_frame = ttk.Frame(main_frame)
    checkbox_frame.grid(row=5, column=0, columnspan=2, pady=5, sticky="ew")

    # Sincronizar Precio
    ttk.Label(checkbox_frame, text="Sincronizar Precio").grid(row=0, column=0, padx=10, pady=5, sticky="w")
    ttk.Radiobutton(checkbox_frame, text="Sí", variable=gestionar_precio, value=True).grid(row=1, column=0, sticky="w")
    ttk.Radiobutton(checkbox_frame, text="No", variable=gestionar_precio, value=False).grid(row=2, column=0, sticky="w")

    # Sincronizar Stock
    ttk.Label(checkbox_frame, text="Sincronizar Stock").grid(row=0, column=2, padx=10, pady=5, sticky="w")
    ttk.Radiobutton(checkbox_frame, text="Sí", variable=gestionar_stock, value=True).grid(row=1, column=2, sticky="w")
    ttk.Radiobutton(checkbox_frame, text="No", variable=gestionar_stock, value=False).grid(row=2, column=2, sticky="w")

    # Crear Productos
    ttk.Label(checkbox_frame, text="Crear Productos").grid(row=0, column=4, padx=10, pady=5, sticky="w")
    ttk.Radiobutton(checkbox_frame, text="Sí", variable=crear_productos, value=True).grid(row=1, column=4, sticky="w")
    ttk.Radiobutton(checkbox_frame, text="No", variable=crear_productos, value=False).grid(row=2, column=4, sticky="w")

    # Productos no existentes en Factusol
    ttk.Label(checkbox_frame, text="Productos no existentes en Factusol").grid(row=0, column=6, padx=10, pady=5, sticky="w")
    ttk.Radiobutton(checkbox_frame, text="Ocultar", variable=accion_no_existentes, value="Ocultar").grid(row=1, column=6, sticky="w")
    ttk.Radiobutton(checkbox_frame, text="Eliminar", variable=accion_no_existentes, value="Eliminar").grid(row=2, column=6, sticky="w")

    # Botón para guardar configuración debajo de los checkboxes
    save_button = ttk.Button(main_frame, text="Guardar Configuración", 
                             command=lambda: guardar_configuracion(db_path, csv_path, gestionar_precio, gestionar_stock, crear_productos, accion_no_existentes, hora_sincronizacion))
    save_button.grid(row=6, column=0, columnspan=2, pady=5, sticky="ew")

    # Botón para sincronización manual
    sync_button = ttk.Button(main_frame, text="Sincronizar Ahora", command=iniciar_sincronizacion, style='Custom.TButton')
    sync_button.grid(row=7, column=0, pady=5, padx=10, sticky="ew")  # Colocado justo después de los checkboxes

    # Botón para cancelar la sincronización
    cancel_button = ttk.Button(main_frame, text="Cancelar", command=cancelar_sincronizacion, style='TButton')
    cancel_button.grid(row=7, column=1, pady=5, padx=10, sticky="ew")  # Al lado del botón de sincronización manual

    hora_frame = ttk.Frame(main_frame)
    hora_frame.grid(row=8, column=0, columnspan=2, pady=5, sticky="ew")

    hora_guardada = obtener_hora_sincronizacion_guardada()

    ttk.Label(main_frame, text="Sincronización Automática", font=("Montserrat", 10, "bold")).grid(row=9, column=0, columnspan=2, pady=(10, 5), sticky="ew")

    ttk.Label(hora_frame, text="Hora de sincronización (HH:MM):", font=montserrat).grid(row=0, column=0, pady=5, padx=(0, 10), sticky=tk.E)
    hora_sincronizacion = tk.StringVar(value=hora_guardada)
    hora_entry = ttk.Entry(hora_frame, textvariable=hora_sincronizacion, width=8, font=montserrat)
    hora_entry.grid(row=0, column=1, pady=5, sticky=tk.W)

    activar_sync_button = ttk.Button(main_frame, text="Activar Sincronización", command=activar_sincronizacion_automatica, style='TButton')
    activar_sync_button.grid(row=10, column=0, pady=5, sticky="ew")

    cancelar_sync_button = ttk.Button(main_frame, text="Cancelar Sincronización", command=cancelar_sincronizacion_automatica, style='TButton')
    cancelar_sync_button.grid(row=10, column=1, pady=5, sticky="ew")

    log_frame = ttk.Frame(main_frame)
    log_frame.grid(row=11, column=0, columnspan=2, pady=5, sticky=(tk.W, tk.E, tk.N, tk.S))

    log_scrollbar = tk.Scrollbar(log_frame, orient=tk.VERTICAL)
    log_text = tk.Text(log_frame, wrap='word', font=montserrat, borderwidth=2, relief="solid", yscrollcommand=log_scrollbar.set)
    log_scrollbar.config(command=log_text.yview)
    log_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    buscar_frame = ttk.Frame(root)
    buscar_frame.grid(row=12, column=0, pady=5, sticky=tk.EW)

    ttk.Label(buscar_frame, text="Buscar SKU o palabra clave:", font=montserrat).grid(row=0, column=0, padx=(10, 5))

    buscar_entry = ttk.Entry(buscar_frame, font=montserrat)
    buscar_entry.grid(row=0, column=1, padx=(0, 10))

    buscar_button = ttk.Button(buscar_frame, text="Buscar", style='TButton')
    buscar_button.grid(row=0, column=2, padx=(0, 10))

    anterior_button = ttk.Button(buscar_frame, text="Anterior", style='TButton')
    anterior_button.grid(row=0, column=3, padx=(0, 10))

    siguiente_button = ttk.Button(buscar_frame, text="Siguiente", style='TButton')
    siguiente_button.grid(row=0, column=4, padx=(0, 10))

    text_handler = TextHandler(log_text)
    logging.getLogger().addHandler(text_handler)

    buscar_button.config(command=lambda: text_handler.buscar_en_logs(buscar_entry.get()))
    anterior_button.config(command=text_handler.anterior_coincidencia)
    siguiente_button.config(command=text_handler.siguiente_coincidencia)

    def abrir_enlace(event):
        webbrowser.open_new("https://tiendapocket.com/")

    footer = tk.Label(root, text="Desarrollado por Tienda Pocket", font=("Montserrat", 9), fg="blue", cursor="hand2")
    footer.grid(row=13, column=0, pady=5)
    footer.bind("<Button-1>", abrir_enlace)

    root.grid_rowconfigure(1, weight=1)
    root.grid_columnconfigure(0, weight=1)
    main_frame.grid_rowconfigure(11, weight=1)
    main_frame.grid_columnconfigure(0, weight=1)
    main_frame.grid_columnconfigure(1, weight=1)

    style = ttk.Style()
    style.configure('TButton', font=montserrat, padding=5, relief="flat")
    style.map('TButton', foreground=[('pressed', 'white'), ('active', '#01304f')], background=[('pressed', '#007ACC'), ('active', '#007ACC')])

    root.protocol("WM_DELETE_WINDOW", lambda: (limpiar_estado(), root.destroy()))

    root.mainloop()

if __name__ == "__main__":
    main()
