import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
import threading
import os
import subprocess
import configparser
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), 'scripts', '.env')
load_dotenv(dotenv_path)

# Scheduler
scheduler = BackgroundScheduler()

# Inicializar log_text como global
log_text = None
running_thread = None
stop_event = threading.Event()  # Evento para detener el hilo

def main():
    # Crear la interfaz gráfica
    root = tk.Tk()
    root.title("Sincronizador Tienda Nube")

    # Configurar la fuente Montserrat
    montserrat = ("Montserrat", 10)
    
    # Variables para almacenar las rutas de los archivos seleccionados
    db_path = tk.StringVar()
    csv_path = tk.StringVar()

    # Función para seleccionar la base de datos de Factusol
    def seleccionar_db():
        path = filedialog.askopenfilename(
            title="Seleccionar archivo de base de datos de Factusol",
            filetypes=[("Access Database", "*.mdb;*.accdb"), ("All Files", "*.*")]
        )
        if path:
            db_path.set(path)

    # Función para seleccionar el directorio donde se guardarán los CSV
    def seleccionar_directorio_csv():
        path = filedialog.askdirectory(
            title="Seleccionar directorio para guardar archivos CSV"
        )
        if path:
            csv_path.set(path)

    # Funciones para manejar la sincronización
    def sincronizacion_manual():
        global running_thread
        try:
            log("Iniciando sincronización manual...")
            if stop_event.is_set():
                log("Sincronización cancelada antes de comenzar.")
                return
            run_script(os.path.abspath(os.path.join('scripts', 'export_to_csv.py')), [db_path.get(), csv_path.get()])
            
            if stop_event.is_set():
                log("Sincronización cancelada después de exportar CSV.")
                return
            run_script(os.path.abspath(os.path.join('scripts', 'conversor_json.py')), [
                os.path.join(csv_path.get(), "F_ART.csv"),
                os.path.join(csv_path.get(), "F_LTA.csv"),
                os.path.join(csv_path.get(), "F_STO.csv"),
                os.path.join(csv_path.get(), "F_ARC.csv"),
                os.path.join(csv_path.get(), "F_STC.csv"),
                os.path.join(csv_path.get(), "F_LTC.csv")
            ])
            
            if stop_event.is_set():
                log("Sincronización cancelada después de generar JSON.")
                return
            # Pasar el archivo JSON generado a carga_tn.py
            run_script(os.path.abspath(os.path.join('scripts', 'carga_tn.py')), ['productos.json'])
            log("Sincronización manual completada.")
        except Exception as e:
            log(f"Error en sincronización manual: {e}")
        finally:
            running_thread = None
            stop_event.clear()

    def run_script(script_name, args=[]):
        try:
            log(f"Ejecutando script: {script_name} con argumentos: {args}")
            process = subprocess.Popen(
                ["python", script_name] + args, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True
            )
            while process.poll() is None:
                stdout_line = process.stdout.readline()
                stderr_line = process.stderr.readline()
                if stdout_line:
                    log(f"STDOUT: {stdout_line.strip()}")
                if stderr_line:
                    log(f"STDERR: {stderr_line.strip()}")
                if stop_event.is_set():
                    process.terminate()
                    log("Proceso terminado por solicitud de cancelación.")
                    break

            stdout, stderr = process.communicate()
            if stdout:
                log(f"STDOUT: {stdout.strip()}")
            if stderr:
                log(f"STDERR: {stderr.strip()}")
            rc = process.poll()
            if rc != 0 and not stop_event.is_set():
                log(f"Error al ejecutar {script_name}: Código de retorno {rc}")
        except Exception as e:
            log(f"Error al ejecutar {script_name}: {e}")

    def log(message):
        if log_text:
            log_text.insert(tk.END, message + "\n")
            log_text.see(tk.END)
        print(message)

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

    # Funciones para la sincronización automática
    def activar_sincronizacion_automatica():
        hora = hora_sincronizacion.get()
        if hora:
            scheduler.add_job(sincronizacion_manual, CronTrigger.from_crontab(f"{hora} * * *"))
            scheduler.start()
            log(f"Sincronización automática programada a las {hora}:00 horas.")
        else:
            log("Por favor, seleccione una hora para la sincronización automática.")

    def cancelar_sincronizacion_automatica():
        scheduler.remove_all_jobs()
        log("Sincronización automática cancelada.")

    # Configurar el tamaño mínimo de la ventana para asegurar que el pie de página sea visible
    root.minsize(600, 600)

    # Frame principal
    main_frame = ttk.Frame(root, padding="10")
    main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    # Botones para seleccionar archivos y directorios
    db_button = ttk.Button(main_frame, text="Seleccionar Base de Datos", command=seleccionar_db, style='TButton')
    db_button.grid(row=0, column=0, pady=10)

    csv_button = ttk.Button(main_frame, text="Seleccionar Directorio CSV", command=seleccionar_directorio_csv, style='TButton')
    csv_button.grid(row=0, column=1, pady=10)

    # Mostrar la ruta seleccionada para la base de datos y los CSV
    db_label = ttk.Label(main_frame, textvariable=db_path, font=montserrat)
    db_label.grid(row=1, column=0, pady=5, columnspan=2)

    csv_label = ttk.Label(main_frame, textvariable=csv_path, font=montserrat)
    csv_label.grid(row=2, column=0, pady=5, columnspan=2)

    # Botones de sincronización y cancelación
    sync_button = ttk.Button(main_frame, text="Sincronizar Ahora", command=iniciar_sincronizacion, style='TButton')
    sync_button.grid(row=3, column=0, pady=10)

    cancel_button = ttk.Button(main_frame, text="Cancelar", command=cancelar_sincronizacion, style='TButton')
    cancel_button.grid(row=3, column=1, pady=10)

    # Sección de sincronización automática
    ttk.Label(main_frame, text="Configuración de sincronización automática", font=("Montserrat", 12, "bold")).grid(row=4, column=0, columnspan=2, pady=10)

    ttk.Label(main_frame, text="Hora de sincronización (24h):", font=montserrat).grid(row=5, column=0, pady=5, sticky=tk.W)
    hora_sincronizacion = tk.StringVar()
    hora_entry = ttk.Entry(main_frame, textvariable=hora_sincronizacion, width=5, font=montserrat)
    hora_entry.grid(row=5, column=1, pady=5, sticky=tk.W)

    activar_sync_button = ttk.Button(main_frame, text="Activar Sincronización", command=activar_sincronizacion_automatica, style='TButton')
    activar_sync_button.grid(row=6, column=0, pady=10)

    cancelar_sync_button = ttk.Button(main_frame, text="Cancelar Sincronización", command=cancelar_sincronizacion_automatica, style='TButton')
    cancelar_sync_button.grid(row=6, column=1, pady=10)

    # Log
    global log_text
    log_text = tk.Text(main_frame, wrap='word', height=15, width=80, font=montserrat)
    log_text.grid(row=7, column=0, columnspan=3, pady=10)

    # Pie de página
    footer = ttk.Label(root, text="Desarrollado por Tienda Pocket", font=("Montserrat", 10))
    footer.grid(row=1, column=0, pady=10)

    # Ajustar la distribución de filas y columnas
    root.grid_rowconfigure(0, weight=1)
    root.grid_columnconfigure(0, weight=1)

    # Mantener el pie de página siempre visible
    root.grid_rowconfigure(1, weight=0)

    # Aplicar estilo a los botones
    style = ttk.Style()
    style.configure('TButton', font=montserrat, padding=6, relief="flat")
    style.map('TButton', foreground=[('pressed', 'white'), ('active', 'white')], background=[('pressed', '#007ACC'), ('active', '#007ACC')])

    # Ejecutar la interfaz
    root.mainloop()

if __name__ == "__main__":
    main()



























