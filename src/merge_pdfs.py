# -*- coding: utf-8 -*-
"""
Flujo: Por cada carpeta fuente en Google Drive:
  1) Listar los PDFs (solo nivel de la carpeta, no subcarpetas).
  2) Descargar y unirlos en 1 PDF (orden por fecha de creación ascendente).
  3) (Opcional) Comprimir el PDF resultante con Ghostscript.
  4) Subir el "compilado" a una subcarpeta llamada "Compilados" dentro de ESA MISMA carpeta.
  5) Enviar los PDFs originales a la PAPELERA (no borrado definitivo).
  6) Evita duplicados del mismo día: si ya existe "Compilado de AAAA-MM-DD.pdf" lo manda a papelera y sube el nuevo.

Notas:
- No mezcla nada entre carpetas: cada carpeta genera su propio compilado.
- Si la subcarpeta "Compilados" no existe, se crea automáticamente.
- Maneja PDFs dañados/protegidos: se saltan y sigue con el resto.
- Autenticación por defecto con OAuth (tu cuenta); Service Account solo si trabajas en Unidad compartida.
"""

import os, io, json, datetime, tempfile, logging, subprocess, shutil
from typing import List, Optional

from pypdf import PdfReader, PdfWriter

# Google Drive API
from google.oauth2 import service_account  # usado solo si AUTH_MODE=service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --------- Configuración general (vienen del workflow) ---------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

SCOPES = ["https://www.googleapis.com/auth/drive"]  # lectura/escritura/papelera
AUTH_MODE = os.getenv("AUTH_MODE", "oauth").lower()  # "oauth" (recomendado) | "service_account"

# Variables siempre necesarias
def _req_env(name: str) -> str:
    """Lee var de entorno y falla con mensaje claro si falta."""
    val = os.getenv(name, "").strip()
    if not val:
        raise RuntimeError(f"Falta la variable de entorno '{name}'. "
                           f"¿Creaste el secret en GitHub y lo nombraste exactamente así?")
    return val

FOLDER_IDS = json.loads(_req_env("FOLDER_IDS"))                 # ["id1","id2",...]
MIN_PDFS = int(os.getenv("MIN_PDFS", "2"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
COMPILED_SUBFOLDER_NAME = os.getenv("COMPILED_SUBFOLDER_NAME", "Compilados").strip() or "Compilados"

# Compresión (controlado por env en el workflow)
PDF_COMPRESS = os.getenv("PDF_COMPRESS", "false").lower() == "true"
PDF_QUALITY = os.getenv("PDF_QUALITY", "ebook")  # screen | ebook | printer | prepress | default


# --------- Cliente de Drive ---------
def drive_client():
    """
    Crea cliente Drive según AUTH_MODE:
      - "oauth": usa TU cuenta y almacenamiento (recomendado para Mi unidad).
      - "service_account": útil si trabajas en Unidad compartida (Shared Drive).
    """
    if AUTH_MODE == "oauth":
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request

        client_id = _req_env("GOOGLE_CLIENT_ID")
        client_secret = _req_env("GOOGLE_CLIENT_SECRET")
        refresh_token = _req_env("GOOGLE_REFRESH_TOKEN")

        logging.info(f"OAuth client_id: ****{client_id[-4:]} (len={len(client_id)})")
        logging.info(f"Have refresh_token: {'yes' if len(refresh_token)>10 else 'no'}")

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=SCOPES,
        )
        # Intercambia refresh_token por access_token válido
        creds.refresh(Request())
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    # Service Account (solo si usas Unidad compartida)
    info = json.loads(_req_env("GOOGLE_CREDENTIALS"))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


# --------- Utilidades de Drive ---------
def get_folder_name(drive, folder_id: str) -> str:
    """Devuelve el nombre de una carpeta por su ID."""
    return drive.files().get(fileId=folder_id, fields="name", supportsAllDrives=True).execute()["name"]


def list_pdfs_in_folder(drive, folder_id: str) -> list:
    """
    Lista PDFs directamente dentro de la carpeta (NO incluye subcarpetas).
    Si existe un subfolder “Compilados”, sus archivos NO aparecen aquí.
    """
    query = (
        f"'{folder_id}' in parents and "
        f"mimeType='application/pdf' and trashed=false"
    )
    files = []
    page_token = None
    while True:
        resp = drive.files().list(
            q=query,
            fields="nextPageToken, files(id, name, createdTime, size)",
            pageSize=1000,
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def ensure_compiled_subfolder(drive, parent_folder_id: str) -> str:
    """
    Busca (y si no existe, crea) la subcarpeta de compilados dentro de la carpeta fuente.
    Devuelve el ID de la subcarpeta.
    """
    # 1) Buscar subcarpeta existente
    q = (
        f"'{parent_folder_id}' in parents and "
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='{COMPILED_SUBFOLDER_NAME}' and trashed=false"
    )
    resp = drive.files().list(
        q=q, fields="files(id,name)", pageSize=1,
        includeItemsFromAllDrives=True, supportsAllDrives=True
    ).execute()
    items = resp.get("files", [])
    if items:
        return items[0]["id"]

    # 2) Crear subcarpeta si no existe
    metadata = {
        "name": COMPILED_SUBFOLDER_NAME,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id],
    }
    if DRY_RUN:
        logging.info(f"[DRY_RUN] Crearía subcarpeta '{COMPILED_SUBFOLDER_NAME}' en {parent_folder_id}")
        return "dry_run_subfolder"
    created = drive.files().create(
        body=metadata, fields="id", supportsAllDrives=True
    ).execute()
    logging.info(f"Subcarpeta de compilados creada: {created['id']}")
    return created["id"]


def download_file(drive, file_id: str, dest_path: str) -> str:
    """Descarga un archivo de Drive a dest_path."""
    request = drive.files().get_media(fileId=file_id)
    with io.FileIO(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
    return dest_path


def upload_pdf(drive, folder_id: str, file_path: str, name: str) -> dict:
    """Sube un PDF a la carpeta indicada y devuelve {id, webViewLink}."""
    media = MediaFileUpload(file_path, mimetype="application/pdf", resumable=True)
    file_metadata = {"name": name, "parents": [folder_id]}
    if DRY_RUN:
        logging.info(f"[DRY_RUN] Subiría '{name}' a carpeta {folder_id}")
        return {"id": "dry_run", "webViewLink": "dry_run"}
    created = drive.files().create(
        body=file_metadata, media_body=media, fields="id,webViewLink", supportsAllDrives=True
    ).execute()
    return created


def move_to_trash(drive, file_id: str):
    """Mueve un archivo a la papelera (NO borra definitivo)."""
    if DRY_RUN:
        logging.info(f"[DRY_RUN] PAPELERA -> {file_id}")
        return
    drive.files().update(fileId=file_id, body={"trashed": True}, supportsAllDrives=True).execute()


# --------- Merge local de PDFs ---------
def merge_local_pdfs(paths: List[str], output_path: str) -> str:
    """
    Une una lista de rutas PDF en 'output_path'.
    - Salta PDFs dañados o protegidos, pero continúa con el resto.
    - Lanza error si al final no hay páginas válidas.
    """
    writer = PdfWriter()
    for p in paths:
        try:
            reader = PdfReader(p)
            for page in reader.pages:
                writer.add_page(page)
        except Exception as e:
            logging.warning(f"Saltando PDF corrupto/protegido: {p} ({e})")

    if len(writer.pages) == 0:
        raise RuntimeError("No se pudieron leer páginas válidas para el merge.")

    with open(output_path, "wb") as f:
        writer.write(f)

    return output_path


# --------- Compresión con Ghostscript (opcional) ---------
def compress_pdf_gs(input_path: str, output_path: str, quality: str = "ebook") -> bool:
    """
    Comprime un PDF usando Ghostscript.
    quality: screen | ebook | printer | prepress | default
    Devuelve True si generó 'output_path'.
    """
    gs = shutil.which("gs") or "gs"  # binario Ghostscript
    quality = (quality or "ebook").lower()
    settings_map = {
        "screen": "/screen",
        "ebook": "/ebook",
        "printer": "/printer",
        "prepress": "/prepress",
        "default": "/default",
    }
    level = settings_map.get(quality, "/ebook")

    cmd = [
        gs,
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.5",
        f"-dPDFSETTINGS={level}",
        "-dDetectDuplicateImages=true",
        "-dDownsampleColorImages=true",
        "-dColorImageDownsampleType=/Bicubic",
        "-dColorImageResolution=150",
        "-dDownsampleGrayImages=true",
        "-dGrayImageDownsampleType=/Bicubic",
        "-dGrayImageResolution=150",
        "-dDownsampleMonoImages=true",
        "-dMonoImageDownsampleType=/Subsample",
        "-dMonoImageResolution=300",
        "-dNOPAUSE", "-dBATCH", "-dQUIET",
        f"-sOutputFile={output_path}",
        input_path,
    ]
    try:
        subprocess.run(cmd, check=True)
        return os.path.exists(output_path) and os.path.getsize(output_path) > 0
    except Exception as e:
        logging.warning(f"Compresión Ghostscript falló: {e}")
        return False


# --------- Evitar duplicado del día ---------
def trash_existing_compiled_for_today(drive, compiled_folder_id: str, date_str: str):
    """
    Si ya existe 'Compilado de YYYY-MM-DD.pdf' en la subcarpeta, lo manda a papelera.
    Así evitamos duplicados si corres el flujo dos veces el mismo día.
    """
    name = f"Compilado de {date_str}.pdf"
    q = f"'{compiled_folder_id}' in parents and name='{name}' and trashed=false"
    resp = drive.files().list(
        q=q, fields="files(id,name)", pageSize=10,
        includeItemsFromAllDrives=True, supportsAllDrives=True
    ).execute()
    for f in resp.get("files", []):
        drive.files().update(fileId=f["id"], body={"trashed": True}, supportsAllDrives=True).execute()
        logging.info(f"Duplicado previo enviado a papelera: {f['name']} ({f['id']})")


# --------- Proceso de una carpeta ---------
def process_folder(drive, folder_id: str) -> Optional[dict]:
    """Procesa una carpeta: mergea sus PDFs, comprime (si aplica) y manda originales a papelera."""
    folder_name = get_folder_name(drive, folder_id)
    logging.info(f"== Carpeta fuente: {folder_name} ({folder_id}) ==")

    # 1) Listar PDFs
    pdfs = list_pdfs_in_folder(drive, folder_id)
    logging.info(f"PDFs encontrados: {len(pdfs)}")

    # 2) Si hay pocos PDFs, evitamos crear compilados vacíos o triviales
    if len(pdfs) < MIN_PDFS:
        logging.info(f"Menos de {MIN_PDFS} PDFs. Se omite merge.")
        return None

    # 3) Ordenar por fecha de creación (ascendente). Cambia a 'name' si te conviene.
    pdfs.sort(key=lambda f: f.get("createdTime", ""))

    # 4) Asegurar subcarpeta 'Compilados' dentro de ESTA carpeta
    compiled_folder_id = ensure_compiled_subfolder(drive, folder_id)

    with tempfile.TemporaryDirectory() as tmp:
        local_paths = []
        # 5) Descargar todos los PDFs a /tmp
        for f in pdfs:
            dest = os.path.join(tmp, f["name"])
            download_file(drive, f["id"], dest)
            local_paths.append(dest)

        # 6) Hacer el merge local
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        merged_name = f"Compilado de {date_str}.pdf"  # <-- nombre solicitado
        merged_path = os.path.join(tmp, merged_name)
        merge_local_pdfs(local_paths, merged_path)

        # 6.1) Evitar duplicado del mismo día
        if not DRY_RUN:
            trash_existing_compiled_for_today(drive, compiled_folder_id, date_str)

        # 7) (Opcional) Comprimir con Ghostscript antes de subir
        final_upload_path = merged_path
        if PDF_COMPRESS:
            compressed_path = os.path.join(tmp, "__compressed__.pdf")
            ok = compress_pdf_gs(merged_path, compressed_path, PDF_QUALITY)
            if ok:
                try:
                    orig = os.path.getsize(merged_path)
                    comp = os.path.getsize(compressed_path)
                    if comp < orig * 0.98:  # al menos 2% más pequeño
                        final_upload_path = compressed_path
                        logging.info(f"Comprimido OK: {orig/1024:.1f}KB -> {comp/1024:.1f}KB ({PDF_QUALITY})")
                    else:
                        logging.info("Compresión no redujo tamaño de forma útil; se sube original.")
                except Exception:
                    pass

        # 8) Subir compilado a la subcarpeta 'Compilados'
        uploaded = upload_pdf(drive, compiled_folder_id, final_upload_path, merged_name)
        logging.info(f"Compilado subido: {uploaded.get('webViewLink', 'dry_run')}")

    # 9) Enviar originales a la PAPELERA (no borrar definitivo)
    for f in pdfs:
        move_to_trash(drive, f["id"])

    return uploaded


# --------- Entry point ---------
def main():
    drive = drive_client()
    results = []
    for fid in FOLDER_IDS:
        try:
            res = process_folder(drive, fid)
            if res:
                results.append(res)
        except Exception as e:
            # Importante: si una carpeta falla, seguimos con las demás.
            logging.error(f"Error en carpeta {fid}: {e}")
            continue

    logging.info(f"Terminado. Compilados generados: {len(results)}")


if __name__ == "__main__":
    main()
