# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['main.py'],
    pathex=['.'],  # Asegúrate de que el directorio actual sea parte del path
    binaries=[],
    datas=[
        ('scripts/*.py', 'scripts'),  # Incluye todos los archivos .py dentro de la carpeta scripts
        ('scripts/.env', 'scripts'),  # Incluye el archivo .env dentro de la carpeta scripts
        ('scripts/config.txt', 'scripts'),  # Incluye el archivo config.txt dentro de la carpeta scripts
        ('icon.ico', '.'),  # Incluye el icono en la raíz del ejecutable
    ],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='SincronizadorTiendaNube',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # Cambiado a False para ocultar la consola
    icon='icon.ico',  # Especifica el icono aquí
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='SincronizadorTiendaNube'
)

