import asyncio
import os
import re
import sys
import uuid
import shutil
import logging
import threading
import time
import mimetypes
import webbrowser
from typing import Dict, Any, Optional
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import yt_dlp

# ============================================================
# AMBIENTE
# ============================================================
def is_frozen() -> bool:
    return getattr(sys, "frozen", False) or "__compiled__" in globals()

def resource_path(rel: str) -> str:
    candidates = []
    if hasattr(sys, "_MEIPASS"): candidates.append(sys._MEIPASS)
    if "__compiled__" in globals(): candidates.append(os.path.dirname(os.path.abspath(__file__)))
    candidates.append(os.path.dirname(os.path.abspath(sys.argv[0])))
    candidates.append(os.path.dirname(os.path.abspath(__file__)))
    for base in candidates:
        p = os.path.join(base, rel)
        if os.path.exists(p): return p
    return os.path.join(candidates[-1], rel)

def writable_path(rel: str) -> str:
    base = os.path.dirname(os.path.abspath(sys.argv[0])) if is_frozen() else os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, rel)

IS_SERVER = os.environ.get("RENDER", "").lower() == "true" or os.environ.get("PORT") is not None
HOST = "0.0.0.0" if IS_SERVER else "127.0.0.1"
PORT = int(os.environ.get("PORT", "8000"))
PROXY_URL = os.environ.get("PROXY_URL")

# ============================================================
# LOG
# ============================================================
LOG_FILE = writable_path("neonvd.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ============================================================
# CAMINHOS
# ============================================================
INDEX_FILE = resource_path("index.html")
DOWNLOAD_DIR = writable_path("downloads")
PREVIEW_DIR = writable_path("previews")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(PREVIEW_DIR, exist_ok=True)

# ============================================================
# PROXY SELETIVO
# ============================================================
PROXY_REQUIRED_DOMAINS = ("youtube.com", "youtu.be", "youtube-nocookie.com", "music.youtube.com", "m.youtube.com")

def needs_proxy(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
        if host.startswith("www."): host = host[4:]
        return any(host == d or host.endswith("." + d) for d in PROXY_REQUIRED_DOMAINS)
    except Exception:
        return False

# ============================================================
# COOKIES & BINARIOS
# ============================================================
COOKIES_FILE = None
for c in ("/etc/secrets/cookies.txt", writable_path("cookies.txt"), os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")):
    if os.path.exists(c):
        COOKIES_FILE = c
        logger.info(f"✅ Cookies: {c}")
        break

def find_ffmpeg() -> Optional[str]:
    for base in (writable_path("bin"), resource_path("bin")):
        if os.path.isdir(base):
            for n in ("ffmpeg.exe", "ffmpeg"):
                if os.path.exists(os.path.join(base, n)):
                    return base
    ff = shutil.which("ffmpeg")
    return os.path.dirname(ff) if ff else None

FFMPEG_LOCATION = find_ffmpeg()

def find_deno() -> Optional[str]:
    for base in (writable_path("bin"), resource_path("bin")):
        if os.path.isdir(base):
            for n in ("deno.exe", "deno"):
                if os.path.exists(os.path.join(base, n)):
                    d = base
                    if d not in os.environ.get("PATH", ""): os.environ["PATH"] = d + os.pathsep + os.environ.get("PATH", "")
                    return os.path.join(base, n)
    d = shutil.which("deno")
    if d:
        dd = os.path.dirname(d)
        if dd not in os.environ.get("PATH", ""): os.environ["PATH"] = dd + os.pathsep + os.environ.get("PATH", "")
    return d

DENO_PATH = find_deno()

# ============================================================
# APP
# ============================================================
app = FastAPI(title="NeonVD API", version="2.4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/", include_in_schema=False)
async def serve_index():
    if not os.path.exists(INDEX_FILE): raise HTTPException(status_code=404, detail="index.html não encontrado.")
    return FileResponse(INDEX_FILE)

@app.get("/api/health")
async def health():
    return {"status": "ok", "ffmpeg": bool(FFMPEG_LOCATION), "deno": bool(DENO_PATH), "cookies": bool(COOKIES_FILE), "proxy": bool(PROXY_URL), "server": IS_SERVER}

# ============================================================
# ESTADO E MODELOS
# ============================================================
tasks_state: Dict[str, Dict[str, Any]] = {}
previews_state: Dict[str, Dict[str, Any]] = {}
connections: Dict[str, WebSocket] = {}
state_lock = threading.Lock()
analyze_cache: Dict[str, dict] = {}
CACHE_TTL = 3600

class AnalyzeRequest(BaseModel):
    url: str
class DownloadRequest(BaseModel):
    url: str
    format: str
    quality: str = "best"
class PreviewRequest(BaseModel):
    url: str

# ============================================================
# HELPERS
# ============================================================
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

def clean_ansi(t: str) -> str:
    return ANSI_RE.sub("", str(t or "")).strip()

def base_opts(use_proxy: bool = True) -> Dict[str, Any]:
    opts: Dict[str, Any] = {
        "quiet": True, "no_warnings": True, "http_headers": {"User-Agent": USER_AGENT},
        "noplaylist": True, "concurrent_fragment_downloads": 8, "http_chunk_size": 10 * 1024 * 1024,
        "buffersize": 1024 * 1024, "retries": 10, "fragment_retries": 10, "file_access_retries": 5, "continuedl": True,
    }
    if FFMPEG_LOCATION: opts["ffmpeg_location"] = FFMPEG_LOCATION
    if COOKIES_FILE: opts["cookiefile"] = COOKIES_FILE
    if use_proxy and PROXY_URL: opts["proxy"] = PROXY_URL
    opts["extractor_args"] = {"youtube": {"player_client": ["web_safari", "web", "android", "ios", "tv"]}}
    return opts

MIME_MAP = {
    ".mp4": "video/mp4", ".webm": "video/webm", ".mkv": "video/x-matroska", ".mov": "video/quicktime",
    ".m4v": "video/x-m4v", ".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".opus": "audio/opus",
    ".ogg": "audio/ogg", ".wav": "audio/wav", ".flac": "audio/flac",
}
MEDIA_EXTENSIONS = tuple(MIME_MAP.keys())

def get_mime_type(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    return MIME_MAP.get(ext) or mimetypes.guess_type(filename)[0] or "application/octet-stream"

def cleanup_previews(max_age_hours: int = 1):
    try:
        now = time.time()
        for f in os.listdir(PREVIEW_DIR):
            p = os.path.join(PREVIEW_DIR, f)
            if os.path.isfile(p) and (now - os.path.getmtime(p)) > max_age_hours * 3600: os.remove(p)
    except Exception: pass

# ============================================================
# ANALYZE
# ============================================================
@app.post("/api/analyze")
async def analyze(req: AnalyzeRequest):
    url = req.url.strip()
    logger.info(f"Analisando: {url}")
    cached = analyze_cache.get(url)
    if cached and (time.time() - cached["ts"]) < CACHE_TTL:
        return cached["data"]

    use_proxy = needs_proxy(url) and bool(PROXY_URL)
    opts = base_opts(use_proxy=use_proxy)
    opts["extract_flat"] = False
    opts["format"] = None

    def _run():
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    try:
        info = await asyncio.to_thread(_run)
        result = {
            "title": info.get("title", "Sem título"),
            "thumbnail": info.get("thumbnail", ""),
            "duration": info.get("duration", 0),
            "channel": info.get("uploader", info.get("channel", "Desconhecido")),
            "platform": info.get("extractor_key", "Web"),
        }
        analyze_cache[url] = {"ts": time.time(), "data": result}
        return result
    except Exception as e:
        logger.error(f"Erro analyze: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# ============================================================
# PREVIEW
# ============================================================
def run_preview(preview_id: str, url: str):
    outtmpl = os.path.join(PREVIEW_DIR, f"{preview_id}.%(ext)s")
    use_proxy = needs_proxy(url) and bool(PROXY_URL)
    opts = base_opts(use_proxy=use_proxy)
    opts.update({"outtmpl": outtmpl, "format": "worst[ext=mp4]/worst[height<=360]/worst", "postprocessors": []})
    previews_state[preview_id]["status"] = "downloading"
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
        files = [f for f in os.listdir(PREVIEW_DIR) if f.startswith(preview_id) and f.endswith((".mp4", ".webm"))]
        if not files: raise FileNotFoundError("Preview não encontrado")
        previews_state[preview_id]["path"] = os.path.join(PREVIEW_DIR, files[0])
        previews_state[preview_id]["status"] = "ready"
    except Exception as e:
        previews_state[preview_id]["status"] = "error"
        previews_state[preview_id]["message"] = str(e)

@app.post("/api/preview")
async def start_preview(req: PreviewRequest):
    preview_id = str(uuid.uuid4())
    cleanup_previews(max_age_hours=1)
    with state_lock:
        previews_state[preview_id] = {"status": "queued", "path": None, "url": req.url.strip()}
    asyncio.create_task(asyncio.to_thread(run_preview, preview_id, req.url.strip()))
    return {"preview_id": preview_id, "status": "queued"}

@app.get("/api/preview-status/{preview_id}")
async def preview_status(preview_id: str):
    with state_lock: state = previews_state.get(preview_id)
    if not state: raise HTTPException(status_code=404, detail="Preview não encontrado")
    return {"status": state["status"], "ready": state.get("path") is not None, "message": state.get("message", "")}

@app.get("/api/preview-file/{preview_id}")
async def preview_file(preview_id: str):
    with state_lock: state = previews_state.get(preview_id)
    if not state or not state.get("path"): raise HTTPException(status_code=404, detail="Preview indisponível")
    return FileResponse(path=state["path"], media_type=get_mime_type(state["path"]), headers={"Content-Disposition": "inline", "Accept-Ranges": "bytes"})

# ============================================================
# DOWNLOAD (COM PROTEÇÃO CONTRA .TXT)
# ============================================================
def run_download(task_id: str, url: str, req: DownloadRequest, loop, use_proxy: bool = True):
    outtmpl = os.path.join(DOWNLOAD_DIR, f"{task_id}_%(title).100B.%(ext)s")

    def emit(data: dict):
        with state_lock:
            tasks_state.setdefault(task_id, {}).update(data)
            ws = connections.get(task_id)
        if ws:
            try: asyncio.run_coroutine_threadsafe(ws.send_json(data), loop)
            except Exception: pass

    def hook(d):
        if d.get("status") == "downloading":
            raw = clean_ansi(d.get("_percent_str", "0%")).rstrip("%")
            try: pct = float(raw)
            except ValueError:
                got = d.get("downloaded_bytes") or 0
                tot = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                pct = (got / tot * 100) if tot else 0
            emit({"status": "downloading", "percent": round(pct, 2), "speed": clean_ansi(d.get("_speed_str", "-- MB/s")), "eta": clean_ansi(d.get("_eta_str", "--:--"))})
        elif d.get("status") == "finished":
            emit({"status": "processing", "percent": 99, "message": "Processando..."})

    def build_opts(use_proxy_inner: bool):
        o = base_opts(use_proxy=use_proxy_inner)
        o.update({"outtmpl": outtmpl, "progress_hooks": [hook]})
        if req.format == "audio":
            if req.quality == "audio-original":
                o["format"] = "ba[ext=m4a]/ba[ext=opus]/ba[ext=webm]/ba/b"
            else:
                bitrate = req.quality.replace("audio-", "") if "audio-" in req.quality else "192"
                o["format"] = "ba/b/bestaudio/best"
                o["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": bitrate}]
        else:
            if req.quality == "best": o["format"] = "b[ext=mp4]/b/bv*+ba/bestvideo+bestaudio/best/worst"
            else:
                q = req.quality
                o["format"] = (f"b[ext=mp4][height<={q}]/b[height<={q}]/bv*[height<={q}]+ba/b[height<={q}]/best[height<={q}]/b/worst")
            o["merge_output_format"] = "mp4"
        return o

    try:
        with yt_dlp.YoutubeDL(build_opts(use_proxy)) as ydl:
            ydl.download([url])
    except Exception as e:
        if not use_proxy and PROXY_URL:
            try:
                with yt_dlp.YoutubeDL(build_opts(True)) as ydl:
                    ydl.download([url])
            except Exception as e2:
                emit({"status": "error", "percent": 0, "message": str(e2)})
                return
        else:
            emit({"status": "error", "percent": 0, "message": str(e)})
            return

    try:
        # Procura qualquer arquivo com o task_id
        all_files = [f for f in os.listdir(DOWNLOAD_DIR) if f.startswith(task_id) and not f.endswith((".part", ".temp", ".ytdl"))]
        if not all_files: raise FileNotFoundError("Arquivo não encontrado")

        # 🛡️ PROTEÇÃO: Só aceita se for realmente um vídeo/áudio
        valid_files = [f for f in all_files if f.lower().endswith(MEDIA_EXTENSIONS)]
        
        if not valid_files:
            # Se o proxy falhou e gerou um .txt ou .html, apaga e avisa
            for f in all_files:
                try: os.remove(os.path.join(DOWNLOAD_DIR, f))
                except: pass
            raise ValueError("O proxy ou o site bloqueou o download (arquivo inválido recebido). Tente novamente.")

        valid_files.sort(key=lambda f: os.path.getsize(os.path.join(DOWNLOAD_DIR, f)), reverse=True)
        path = os.path.join(DOWNLOAD_DIR, valid_files[0])
        
        with state_lock:
            tasks_state[task_id].update({"file_path": path, "status": "finished", "percent": 100})
        emit({"status": "finished", "percent": 100, "message": "Concluído!"})
        logger.info(f"[{task_id}] OK: {path}")
    except Exception as e:
        logger.error(f"[{task_id}] Erro pós-download: {e}")
        emit({"status": "error", "percent": 0, "message": str(e)})

@app.post("/api/download")
async def start_download(req: DownloadRequest):
    task_id = str(uuid.uuid4())
    use_proxy = needs_proxy(req.url.strip()) and bool(PROXY_URL)
    with state_lock:
        tasks_state[task_id] = {"status": "queued", "percent": 0, "speed": "--", "eta": "--", "file_path": None, "message": "Na fila...", "using_proxy": use_proxy}
    loop = asyncio.get_running_loop()
    asyncio.create_task(asyncio.to_thread(run_download, task_id, req.url.strip(), req, loop, use_proxy))
    return {"task_id": task_id, "message": "Download iniciado", "using_proxy": use_proxy}

@app.get("/api/progress/{task_id}")
async def progress(task_id: str):
    with state_lock: state = tasks_state.get(task_id)
    if not state: raise HTTPException(status_code=404, detail="Tarefa não encontrada")
    return state

@app.websocket("/api/ws/progress/{task_id}")
async def ws_progress(websocket: WebSocket, task_id: str):
    await websocket.accept()
    with state_lock:
        connections[task_id] = websocket
        current = tasks_state.get(task_id)
    if current:
        try: await websocket.send_json(current)
        except Exception: pass
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect: pass
    finally:
        with state_lock: connections.pop(task_id, None)

@app.get("/api/download-file/{task_id}")
async def download_file(task_id: str, inline: bool = False):
    with state_lock:
        state = tasks_state.get(task_id)
        path = state.get("file_path") if state else None
    if not state: raise HTTPException(status_code=404, detail="Tarefa não encontrada")
    if not path or not os.path.exists(path): raise HTTPException(status_code=404, detail="Arquivo indisponível")
    clean_name = os.path.basename(path).replace(f"{task_id}_", "", 1)
    return FileResponse(path=path, filename=clean_name, media_type=get_mime_type(clean_name),
        headers={"Content-Disposition": f'{"inline" if inline else "attachment"}; filename="{clean_name}"', "Cache-Control": "private, max-age=3600"})

# ============================================================
# START
# ============================================================
def main():
    import uvicorn
    logger.info(f"Servidor: http://{HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT, reload=False, log_level="info")

if __name__ == "__main__":
    main()