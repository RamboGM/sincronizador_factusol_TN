# Factusol Synchronizer

## Overview

Factusol Synchronizer is a desktop and service toolkit that automates product synchronization between a Factusol Microsoft Access database and a Tiendanube storefront. The project combines a Tkinter-based desktop interface for operators with automation services and a lightweight Flask API that can be deployed as part of a Tiendanube app installation flow.

## Key Features

- **Guided desktop configuration** – Manage database and CSV export paths, toggle pricing or stock updates, and decide how to handle missing products from an intuitive Tkinter interface.
- **Scheduled synchronization** – Configure cron-style jobs that export data from Factusol, normalize it to JSON, and push updates to Tiendanube using resilient retry and rate-limit handling logic.
- **CSV export utility** – Bulk export key Factusol tables (F_ART, F_ARC, F_STO, and more) to semicolon-separated CSV files that can be used for offline analysis or as staging data for synchronization.
- **REST endpoints for Tiendanube** – Provide installation and webhook endpoints via Flask so the synchronization service can integrate with Tiendanube’s app ecosystem.
- **Executable packaging** – Bundle the desktop client and its assets into a Windows executable with PyInstaller, including icons and default configuration files.

## Project Structure

```
.
├── app_flask.py          # Flask app exposing installation and webhook endpoints
├── main.py               # Tkinter desktop application and job scheduler
├── scripts/
│   ├── sincronizador.py  # Core synchronization logic and Tiendanube API client
│   └── config.txt        # Default configuration copied to the user profile on first run
├── requirements.txt      # Python dependencies for the full toolkit
└── main.spec             # PyInstaller specification for packaging the desktop app
```

## Requirements

- Python 3.10 or higher
- Microsoft Access ODBC driver installed on the host machine (required for `pyodbc` to read Factusol databases)
- Tiendanube API credentials with permissions to manage products
- (Optional) Virtual environment tooling such as `venv` or `conda`

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/<your-org>/sincronizador_factusol_TN.git
   cd sincronizador_factusol_TN
   ```
2. **Create and activate a virtual environment** (recommended)
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
   ```
3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

### Environment variables

The synchronization service requires Tiendanube API credentials defined in `scripts/.env`:

```
ACCESS_TOKEN=<tiendanube-private-access-token>
USER_ID=<tiendanube-store-id>
```

Copy `scripts/.env` from a secure location or create it manually before running the synchronizer.

### Runtime configuration file

At startup the desktop client creates a user-scoped configuration file (`%LOCALAPPDATA%/sincronizador_app/config.txt` on Windows or `~/.config/sincronizador_app/config.txt` on Linux/macOS) by copying the defaults from `scripts/config.txt`. You can edit these settings through the user interface or by editing the file directly:

```
[DEFAULT]
db_path = <path-to-factusol.mdb>
csv_path = <path-to-export-directory>
hora_sincronizacion = <HH:MM in 24h format>
gestionar_precio = False | True
gestionar_stock = False | True
crear_productos = False | True
accion_no_existentes = Ocultar | Eliminar | Desactivar
```

## Usage

### Launch the desktop synchronizer

Run the Tkinter application locally:

```bash
python main.py
```

From the interface you can:
- Select the Factusol `.mdb/.accdb` file and a CSV export folder.
- Toggle whether price, stock, or new-product creation are part of the sync job.
- Schedule automatic synchronization at a specific time or trigger exports manually.
- Review detailed logs, search within them, and monitor counts of created, updated, or removed products.

### Run the Flask endpoints

To expose the installation and webhook endpoints locally:

```bash
python app_flask.py
```

The server listens on `http://0.0.0.0:5000/` and includes placeholder routes for `/install` and `/webhook` that you can extend to match your Tiendanube integration requirements.

## Building a standalone executable

PyInstaller is configured via `main.spec` to package the desktop application, bundling the default configuration and `.env` file:

```bash
pyinstaller main.spec
```

The resulting executable will be available under the `dist/` directory, ready for distribution to Windows operators.

## Logging and Monitoring

The desktop client routes synchronization logs to the GUI, allowing operators to review activity, search through historical messages, and navigate between matches. Service-side logging is emitted to stdout and can be redirected to centralized monitoring when deployed on servers.

## Contributing

1. Fork the repository and create feature branches for your changes.
2. Follow Python best practices and keep dependencies updated in `requirements.txt`.
3. Run linting or formatting tools you rely on before opening a pull request.
4. Submit a detailed PR describing the motivation and testing performed.

## License

This project is currently unlicensed. Contact the repository owner if you plan to use it beyond internal deployments.

