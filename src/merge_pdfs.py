# -*- coding: utf-8 -*-
"""
Flujo: Por cada carpeta fuente en Google Drive:
  1) Listar los PDFs (solo nivel de la carpeta, no subcarpetas).
  2) Descargar y unirlos en 1 PDF (orden por fecha de creación ascendente).
  3) Subir el "compilado" a una subcarpeta llamada "Compilados" dentro de ESA MISMA carpeta.
  4) Enviar los PDFs originales a la PAPELERA (no borrado definitivo).

Notas:
- No mezcla nada entre carpetas: cada carpeta tiene su propio compilado.
- Si la subcarpeta "Compilados" no existe, se crea automáticamente.
- Maneja PDFs dañados/protegidos: se saltan y sigue con el resto.
"""

import os, io, json, datetime, tempfile, logging
from typing import List, Optional

from pypdf import PdfReader, PdfWriter

# Autenticación Google
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --------- Configuración general por variables de entorno (vienen del workflow) ---------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

SCOPES = ["https://www.googleapis.com/auth/drive"]  # lectura/escritura/papelera

# <<< MODO DE AUTENTICACIÓN >>>
AUTH_MODE = os.getenv("AUTH_MODE", "oauth").lower()   # "oauth" por defecto

# Estas sí las usamos siempre
FOLDER_IDS = json.loads(os.environ["FOLDER_IDS"])           # ["id1","id2",...]
MIN_PDFS = int(os.getenv("MIN_PDFS", "2"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
COMPILED_SUBFOLDER_NAME = os.getenv("COMPILED_SUBFOLDER_NAME", "Compilados").strip() or "Compilados"


# --------- Utilidades de Google Drive ---------
def drive_client():
    """
    Crea cliente Drive según AUTH_MODE:
      - "oauth": usa TU cuenta (almacenamiento de tu Drive)
      - "service_account": usa Service Account (solo útil si trabajas en Unidad compartida)
    """
    if AUTH_MODE == "oauth":
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request

        client_id = os.environ["GOOGLE_CLIENT_ID"]
        client_secret = os.environ["GOOGLE_CLIENT_SECRET"]
        refresh_token = os.environ["GOOGLE_REFRESH_TOKEN"]

        creds = Credentials(
            None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=SCOPES,
        )
        creds.refresh(Request())  # cambia el refresh token por un access token válido
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    # Rama opcional por si algún día vuelves a Service Account (Unidad compartida)
    info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)



def get_folder_name(drive, folder_id: str) -> str:
    """Devuelve el nombre de una carpeta por su ID."""
    return drive.files().get(fileId=folder_id, fields="name").execute()["name"]


def list_pdfs_in_folder(drive, folder_id: str) -> list:
    """
    Lista PDFs directamente dentro de la carpeta (NO incluye subcarpetas).
    Importante: como en Drive un archivo solo “pertenece” a su carpeta padre inmediata,
    si creamos un subfolder “Compilados”, sus archivos no aparecen aquí.
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
            pageToken=page_token
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
    resp = drive.files().list(q=q, fields="files(id,name)", pageSize=1).execute()
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
    created = drive.files().create(body=metadata, fields="id").execute()
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
        body=file_metadata, media_body=media, fields="id,webViewLink"
    ).execute()
    return created


def move_to_trash(drive, file_id: str):
    """Mueve un archivo a la papelera (NO borra definitivo)."""
    if DRY_RUN:
        logging.info(f"[DRY_RUN] PAPELERA -> {file_id}")
        return
    drive.files().update(fileId=file_id, body={"trashed": True}).execute()


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


# --------- Proceso de una carpeta ---------
def process_folder(drive, folder_id: str) -> Optional[dict]:
    """Procesa una carpeta: mergea sus PDFs y manda originales a papelera."""
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
        # Fecha 
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
        # Nombre solicitado
        merged_name = f"Compilado__{folder_name}__{timestamp}.pdf"
        merged_path = os.path.join(tmp, merged_name)
        merge_local_pdfs(local_paths, merged_path)

        # 7) Subir compilado a la subcarpeta 'Compilados'
        uploaded = upload_pdf(drive, compiled_folder_id, merged_path, merged_name)
        logging.info(f"Compilado subido: {uploaded.get('webViewLink', 'dry_run')}")

    # 8) Enviar originales a la PAPELERA (no borrar definitivo)
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
