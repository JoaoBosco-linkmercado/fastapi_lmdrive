from __future__ import annotations

from itsdangerous import URLSafeSerializer

from datetime import datetime, timezone, timedelta
from pathlib import Path
import mimetypes
import os
import anyio
import subprocess
import tempfile
import shutil

from urllib.parse import quote
from contextlib import asynccontextmanager

from cryptography.fernet import Fernet

from fastapi import FastAPI, Request, Depends, UploadFile, File

from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    PlainTextResponse,
    StreamingResponse,
    JSONResponse,
    FileResponse,
)
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from werkzeug.utils import secure_filename

import config


from controllers import ctl_wasabi


# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
SESSION_SECRET = os.getenv("SESSION_SECRET", "LM Wasabi Drive versão 1")

# Your Flask app had MAX_CONTENT_LENGTH = 2GB.
MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", str(2 * 1024 * 1024 * 1024)))

TEMPLATE_DIR = os.getenv("TEMPLATE_DIR", "templates")

# 12h session lifetime (Flask used permanent_session_lifetime = 12h).
SESSION_MAX_AGE = int(os.getenv("SESSION_MAX_AGE", str(int(timedelta(hours=12).total_seconds()))))

maxFileNameLength = 64


# ------------------------------------------------------------------------------
# Middleware: basic request size guard (Content-Length based)
# ------------------------------------------------------------------------------
class MaxBodySizeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        cl = request.headers.get("content-length")
        if cl is not None:
            try:
                if int(cl) > MAX_CONTENT_LENGTH:
                    return PlainTextResponse("Request too large", status_code=413)
            except ValueError:
                pass
        return await call_next(request)


@asynccontextmanager
async def lifespan(app: FastAPI):

    print("IN")
    #await carrega_globais(app)
    yield
    #await connect_db.disconnect()
    print("OUT")


# ------------------------------------------------------------------------------
# App setup
# ------------------------------------------------------------------------------

templates = Jinja2Templates(directory=TEMPLATE_DIR)

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    max_age=SESSION_MAX_AGE,
    same_site="lax",
)
app.add_middleware(MaxBodySizeMiddleware)
app.mount("/static", StaticFiles(directory="static"), name="static")


# ------------------------------------------------------------------------------
# File type icons (same as Flask app)
# ------------------------------------------------------------------------------

tp_dict = {
    "image": [["png", "jpg", "svg", "jpeg", "png", "gif", "bmp", "raw"], "fa fa-file-image"],
    "audio": [["mp3", "wav", "ogg", "mpeg", "aac", "3gpp", "3gpp2", "aiff", "x-aiff", "amr", "mpga", "m4a","flac"], "fa fa-file-audio"],
    "video": [["mp4", "webm", "opgg", "flv", "mov", "mkv"], "fa fa-file-video"],
    "pdf": [["pdf"], "fa fa-file-pdf"],
    "ppt": [["ppt", "pptx", "odp"], "fa fa-file-powerpoint"],
    "doc": [["docx", "doc", "odt"], "fa fa-file-word"],
    "excel": [["xls", "xlsx", "ods"], "fa fa-file-excel"],
    "text": [["txt", "rtf", "csv", "log", "xml", "md"], "fa fa-file-text"],
    "compressed": [["zip", "rar", "7z"], "fa fa-file-zip"],
    "code": [["css", "scss", "html", "py", "js", "cpp"], "fa fa-file-code"],
}


def get_mime_type(file_extension: str) -> str | None:
    mime_type, _ = mimetypes.guess_type(f"file.{file_extension}")
    return mime_type


def nocache_headers() -> dict[str, str]:
    now = datetime.now(timezone.utc)
    last_modified = now.strftime("%a, %d %b %Y %H:%M:%S GMT")
    return {
        "Last-Modified": last_modified,
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "-1",
    }


def render(request: Request, template_name: str, **context) -> HTMLResponse:
    return templates.TemplateResponse(template_name, {"request": request, **context})


def redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=302)


# ------------------------------------------------------------------------------
# Dependencies (login + common session fields)
# ------------------------------------------------------------------------------

def require_login(request: Request) -> dict:
    if "login" not in request.session:
        # We'll transform this into a redirect in the exception handler below.
        raise StarletteHTTPException(status_code=307, detail="Not logged in")
    return request.session


def session_ctx(session: dict = Depends(require_login)) -> dict:
    return {
        "login": session.get("login", ""),
        "title": session.get("title", ""),
        "user": session.get("user", ""),
        "internal": session.get("internal", "Y"),
        "external_link": session.get("external_link", session.get("internal", "Y")),
        "sort_by_selected": session.get("sort_by_selected", 0),
        "sort_order": session.get("sort_order", 0),
    }


# ------------------------------------------------------------------------------
# Sorting / directory listing helpers
# ------------------------------------------------------------------------------

def sort_structure(session: dict, all_dir: list[dict]) -> list[dict]:
    sort_by_selected = session.get("sort_by_selected", 0)
    sort_order = session.get("sort_order", 0)

    reverse = bool(sort_order)

    if sort_by_selected == 0:
        return sorted(all_dir, key=lambda x: x["f"].lower(), reverse=reverse)
    if sort_by_selected == 1:
        return sorted(all_dir, key=lambda x: x.get("filetype", ""), reverse=reverse)
    if sort_by_selected == 2:
        return sorted(all_dir, key=lambda x: x.get("dtm_b"), reverse=reverse)
    return sorted(all_dir, key=lambda x: x.get("size_b", 0), reverse=reverse)


def getDirList(session: dict, curDir: str, listdir: list[dict]) -> list[dict]:
    all_dir: list[dict] = []

    # folders first
    for i in [dList for dList in listdir if dList.get("isdir", False)]:
        dots = "..." if len(i.get("name", "")) > maxFileNameLength else ""
        temp_dir = {
            "f": i["name"][0:maxFileNameLength] + dots,
            "f_url": i["obj"],
            "filetype": "",
            "mediatype": "",
            "currentDir": curDir,
            "system": i.get("system", False),
            "isdir": i.get("isdir", False),
            "icon": "fa fa-folder",
            "user": i.get("user", ""),
            "dtm": "",
            "dtm_b": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "size": "---",
            "size_b": 0,
        }
        if i["name"] not in [".", ".."]:
            all_dir.append(temp_dir)

    # files
    for i in [dList for dList in listdir if not dList.get("isdir", False)]:
        icon = None
        mediatype = ""
        tp = i.get("type")
        for k, file_type in tp_dict.items():
            if tp in file_type[0]:
                mediatype = k
                icon = file_type[1]
                break
        if not icon:
            icon = "fa fa-file"

        dots = "..." if len(i.get("name", "")) > maxFileNameLength else ""
        filesizeK = i.get("size", 0) / 1024.0

        modified_dt = i.get("modified", datetime.now(timezone.utc))
        if getattr(modified_dt, "tzinfo", None) is None:
            modified_dt = modified_dt.replace(tzinfo=timezone.utc)

        temp_file = {
            "f": i["name"][0:maxFileNameLength] + dots,
            "filetype": i.get("type", ""),
            "mediatype": mediatype,
            "f_url": i["obj"],
            "currentDir": curDir,
            "system": i.get("system", False),
            "isdir": i.get("isdir", False),
            "icon": icon,
            "user": i.get("user", ""),
            "dtm": modified_dt.strftime("%d/%m/%Y %H:%M:%S"),
            "dtm_b": modified_dt,
            "size": ("%.2fK" % filesizeK)
            if filesizeK < 1000
            else ("%.2fM" % (filesizeK / 1024))
            if (filesizeK / 1024) < 1000
            else ("%.2fG" % (filesizeK / 1024 / 1024)),
            "size_b": i.get("size", 0),
        }
        all_dir.append(temp_file)

    return sort_structure(session, all_dir)


# ------------------------------------------------------------------------------
# Error handling
# ------------------------------------------------------------------------------

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return render(request, "blank.html", errorCode=404, errorText="Page Not Found")
    if exc.status_code == 307 and exc.detail == "Not logged in":
        return redirect("/blank")
    return PlainTextResponse(str(exc.detail), status_code=exc.status_code)


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.get("/blank", response_class=HTMLResponse)
async def blank_page(request: Request):
    resp = render(request, "blank.html")
    resp.headers.update(nocache_headers())
    return resp


@app.post("/login/")
@app.post("/login/{var:path}")
async def login_method(var: str = ""):
    return redirect("/blank")


@app.get("/login/{var:path}", response_class=HTMLResponse)
async def login_get(request: Request, var: str):
    if not var:
        var = "gAAAAABpe7I_Hm7fpMWCi0xXWulW3bcZzs4DyIB2eIrag-zDx48uyHWfXWt75qdyiqD11Upv7ENOxIUt4q7q1z1npa677QTT7tnkikNJoey-M0LtBTGNP5ZBIeZsRqCodKrvLCWkMipxi7yO2t2KSftZoyMC1Z_jZ47J7LtdjtT0hqGFmf8YFrImNuLao05yviXpVvocPal8"
    try:
        f = Fernet(config.app_key)
        connect_cmd = f.decrypt(var.encode("utf-8")).decode("utf-8")

        if connect_cmd.startswith("LMDRIVE:"):
            cmd = connect_cmd.replace("LMDRIVE:", "").split("|")
            request.session["login"] = cmd[0]
            request.session["title"] = cmd[1]
            request.session["user"] = cmd[2]
            request.session["internal"] = cmd[3] if len(cmd) > 3 else "Y"
            request.session["external_link"] = cmd[4] if len(cmd) > 4 else request.session["internal"]
            request.session["sort_by_selected"] = 0
            request.session["sort_order"] = 0

            ctl_wasabi.Wasabi(request.session["login"]).initialize_folder()
            return redirect("/files/")
    except Exception:
        pass

    for k in ["login", "title", "user", "internal", "external_link", "sort_by_selected", "sort_order"]:
        request.session.pop(k, None)

    resp = render(request, "blank.html")
    resp.headers.update(nocache_headers())
    return resp


@app.get("/external_use/", response_class=HTMLResponse)
async def external_use(request: Request, ctx: dict = Depends(session_ctx)):
    f = Fernet(config.app_key)
    token = f.encrypt(
        f"LMDRIVE:{ctx['login']}/Área_do_Cliente|{ctx['title']}|:{ctx['user']}|N".encode("utf-8")
    ).decode("utf-8")

    resp = render(
        request,
        "external_link.html",
        internal_user=ctx["internal"],
        gera_external_link=ctx["external_link"],
        header=ctx["title"],
        external_link=f"https://drive.linkmercado.com.br/login/{token}",
    )
    resp.headers.update(nocache_headers())
    return resp


@app.get("/logout/", response_class=HTMLResponse)
async def logout_method(request: Request):
    for k in ["login", "title", "user", "sort_by_selected", "sort_order", "internal", "external_link"]:
        request.session.pop(k, None)
    resp = render(request, "blank.html")
    resp.headers.update(nocache_headers())
    return resp


@app.get("/changeSort")
async def toggle_sort(request: Request, ctx: dict = Depends(session_ctx), col: str = "name"):
    sort_order = request.session.get("sort_order", 0)

    if col == "name":
        sort_by_selected = 0
    elif col == "filetype":
        sort_by_selected = 1
    elif col == "data":
        sort_by_selected = 2
    else:
        sort_by_selected = 3

    sort_order = 1 if sort_order == 0 else 0

    request.session["sort_by_selected"] = sort_by_selected
    request.session["sort_order"] = sort_order
    return PlainTextResponse("OK")


@app.get("/move")
async def move(request: Request, ctx: dict = Depends(session_ctx), from_: str = "", to_: str = ""):
    if from_ and to_ and to_ != "null" and (from_ not in to_):
        wasabi = ctl_wasabi.Wasabi(ctx["login"])
        if from_.endswith("/"):
            if to_ == "/":
                to_ = ""
            directory_path = Path(from_)
            wasabi.move_folder(from_, to_ + directory_path.name + "/")
            return PlainTextResponse("/files/" + to_)
        else:
            directory_path = Path(from_)
            dir_ = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else "/"
            ok = wasabi.move_file(origin_subfolder_path=dir_, dest_subfolder_path=to_, filename=directory_path.name)
            return PlainTextResponse("OK" if ok else "NOK")
    return PlainTextResponse("OK")


@app.get("/rename/{var:path}")
async def rename(request: Request, var: str, ctx: dict = Depends(session_ctx), name: str = "", obj: str = ""):
    directory_path = Path(var)
    old_name = var
    new_name = name

    if new_name and obj and old_name not in ctl_wasabi.diretorios_sistema:
        wasabi = ctl_wasabi.Wasabi(ctx["login"])
        if obj == "folder":
            ok = wasabi.move_folder(old_name, old_name.replace(directory_path.name, new_name))
        elif obj == "file":
            dir_ = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else "/"
            new_name += directory_path.suffix
            ok = wasabi.rename_file(subfolder_path=dir_, filename=directory_path.name, new_filename=new_name)
        else:
            ok = True
        return PlainTextResponse("OK" if ok else "NOK")
    return PlainTextResponse("OK")


@app.get("/files/", response_class=HTMLResponse)
@app.get("/files/{var:path}", response_class=HTMLResponse)
async def file_page(request: Request, var: str = "", ctx: dict = Depends(session_ctx)):
    breadcrumb: list[list[str]] = []
    wasabi = ctl_wasabi.Wasabi(ctx["login"])
    dir_content = getDirList(request.session, var, wasabi.list_folder(var))

    cList = "" if not var else (var[:-1].split("/") if var.endswith("/") else var.split("/"))
    home_page = "Área do Cliente" if "Área do Cliente" in ctx["login"] else "Home"

    breadcrumb.append(["/files/", home_page, "/"])
    cPath = ""
    for c in cList:
        cPath += f"{c}/"
        breadcrumb.append(["/files/" + cPath, c, cPath])

    return templates.TemplateResponse("home.html", 
        {
            "request": request, 
            "homepage":"Y",
            "internal_user":ctx["internal"],
            "gera_external_link":ctx["external_link"],
            "header":ctx["title"],
            "currentDir":var,
            "breadcrumb":breadcrumb,
            "all_dir":dir_content
        })
    
    #resp.headers.update(nocache_headers())
    #return resp


@app.post("/find/", response_class=HTMLResponse)
@app.post("/find/{var:path}", response_class=HTMLResponse)
async def find(request: Request, var: str = "", ctx: dict = Depends(session_ctx)):
    form = await request.form()
    name = form.get("search_name", "")

    wasabi = ctl_wasabi.Wasabi(ctx["login"])
    dir_content = getDirList(
        request.session,
        var,
        wasabi.find_file(folder_path=var, searched_name=name),
    )

    breadcrumb: list[list[str]] = []
    cList = "" if not var else (var[:-1].split("/") if var.endswith("/") else var.split("/"))
    home_page = "Área do Cliente" if "Área do Cliente" in ctx["login"] else "Home"
    breadcrumb.append(["/files/", home_page, "/"])
    cPath = ""
    for c in cList:
        cPath += f"{c}/"
        breadcrumb.append(["/files/" + cPath, c, cPath])

    shown_var = var or "Home"

    resp = render(
        request,
        "home.html",
        homepage="N",
        mensagem=f"Resultado da busca por <u><i>{name}</i></u> em <u><strong>{shown_var}</strong></u>",
        internal_user=ctx["internal"],
        header=ctx["title"],
        currentDir="",
        breadcrumb=breadcrumb,
        all_dir=dir_content,
    )
    resp.headers.update(nocache_headers())
    return resp


@app.get("/", response_class=HTMLResponse)
async def home_page(request: Request, ctx: dict = Depends(session_ctx)):
    return redirect("/files/")


@app.get("/browse/{var:path}")
@app.get("/download/{var:path}")
async def browse_or_download(request: Request, var: str, ctx: dict = Depends(session_ctx)):
    download = request.url.path.startswith("/download/")
    directory_path = Path(var)
    dir_ = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else "/"

    wasabi = ctl_wasabi.Wasabi(ctx["login"])
    obj = wasabi.get_object(dir_, directory_path.name)
    if not obj:
        raise StarletteHTTPException(status_code=404, detail="Object not found")

    byte_stream = obj["Body"]  # boto StreamingBody (file-like)
    media_type = get_mime_type(directory_path.suffix[1:]) or "application/octet-stream"

    headers = {}
    headers["Content-Disposition"] = (
        f'attachment; filename="{directory_path.name}"' if download else f'inline; filename="{directory_path.name}"'
    )

    resp = StreamingResponse(byte_stream, media_type=media_type, headers=headers)
    resp.headers.update(nocache_headers())
    return resp

@app.get("/pptpdf/{var:path}")
async def ppt_to_pdf(
    request: Request,
    var: str,
    ctx: dict = Depends(session_ctx),
):
    """
    Converte PPT/PPTX para PDF via LibreOffice (headless) e retorna inline.
    Ubuntu / sem cache.
    """

    path = Path(var)
    ext = path.suffix.lower()

    if ext not in (".ppt", ".pptx", ".odp"):
        raise StarletteHTTPException(status_code=400, detail="Arquivo não é PPT/PPTX")

    # resolve diretório + nome (mesmo padrão do /browse)
    dir_ = str(path.parents[0]) if len(path.parents) > 1 else "/"

    wasabi = ctl_wasabi.Wasabi(ctx["login"])
    obj = wasabi.get_object(dir_, path.name)
    if not obj:
        raise StarletteHTTPException(status_code=404, detail="Arquivo não encontrado")

    tmpdir = tempfile.mkdtemp(prefix="pptpdf_")

    try:
        src_path = Path(tmpdir) / path.name
        out_dir = Path(tmpdir)

        # salva o PPT/PPTX localmente
        body = obj["Body"]
        with open(src_path, "wb") as f:
            f.write(body.read())

        # profile isolado do LibreOffice
        user_profile = Path(tmpdir) / "lo_profile"

        # ambiente controlado (evita javaldx)
        env = os.environ.copy()
        env["SAL_USE_VCLPLUGIN"] = "svp"
        env["JAVA_HOME"] = ""
        env["JRE_HOME"] = ""

        cmd = [
            "soffice",
            f"-env:UserInstallation=file://{user_profile.as_posix()}",
            "--headless",
            "--invisible",
            "--nologo",
            "--nofirststartwizard",
            "--norestore",
            "--convert-to", "pdf",
            "--outdir", str(out_dir),
            str(src_path),
        ]

        def _run():
            return subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                check=False,
                timeout=60,  # evita travar
            )

        res = await anyio.to_thread.run_sync(_run)

        pdf_path = out_dir / (src_path.stem + ".pdf")

        if not pdf_path.exists():
            err = (res.stderr or b"").decode("utf-8", "ignore")
            raise StarletteHTTPException(
                status_code=500,
                detail="Falha ao converter PPT/PPTX para PDF: " + err[:500],
            )

        headers = {
            "Content-Disposition": f'inline; filename="{src_path.stem}.pdf"'
        }

        resp = FileResponse(
            str(pdf_path),
            media_type="application/pdf",
            headers=headers,
        )

        # mantém o mesmo comportamento de no-cache do /browse
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"

        return resp

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@app.get("/delete/{var:path}")
async def delete_file(request: Request, var: str, ctx: dict = Depends(session_ctx)):
    directory_path = Path(var)
    dir_ = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else "/"
    ctl_wasabi.Wasabi(ctx["login"]).delete(bucket_dir=dir_, file_name=directory_path.name)
    return redirect("/files" + (dir_ if dir_.startswith("/") else "/" + dir_))


@app.get("/downloadFolder/")
@app.get("/downloadFolder/{var:path}")
async def download_folder(request: Request, var: str = "", ctx: dict = Depends(session_ctx)):
    wasabi = ctl_wasabi.Wasabi(ctx["login"])

    dirlist = wasabi.list_folder(var)
    for f in getDirList(request.session, var, dirlist):
        if not f["isdir"]:
            directory_path = Path(f["f_url"])
            dir_ = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else "/"

            obj = wasabi.get_object(dir_, directory_path.name)
            if not obj:
                continue

            byte_stream = obj["Body"]
            media_type = get_mime_type(directory_path.suffix[1:]) or "application/octet-stream"
            headers = {"Content-Disposition": f'attachment; filename="{directory_path.name}"'}

            resp = StreamingResponse(byte_stream, media_type=media_type, headers=headers)
            resp.headers.update(nocache_headers())
            return resp

    return redirect("/files/" + var)


@app.get("/deleteFolder/")
@app.get("/deleteFolder/{var:path}")
async def delete_folder(request: Request, var: str = "", ctx: dict = Depends(session_ctx)):
    var = var.split("//")[1] if "//" in var else var
    directory_path = Path(var)
    dir_ = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else ""
    ctl_wasabi.Wasabi(ctx["login"]).delete_folder(var)
    return redirect("/files/" + dir_)


@app.post("/api/upload/", response_class=JSONResponse)
@app.post("/api/upload/{var:path}", response_class=JSONResponse)
async def api_upload_file(var: str = "", file: UploadFile = File(...), ctx: dict = Depends(session_ctx)):
    """Single-file upload endpoint for XHR progress UI."""
    var = var.split("//")[1] if "//" in var else var
    safe_name = secure_filename(file.filename or "")
    if not safe_name:
        return JSONResponse({"ok": False, "error": "Nome de arquivo inválido"}, status_code=400)

    wasabi = ctl_wasabi.Wasabi(ctx["login"])

    try:
        w = await anyio.to_thread.run_sync(
            wasabi.put_object,
            var,
            safe_name.rstrip(),
            file.file,
            ctx.get("user"),
        )
        status = (w or {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        ok = status in (200, 204)
        if not ok:
            return JSONResponse({"ok": False, "name": safe_name, "error": f"HTTPStatusCode={status}"}, status_code=500)
        return {"ok": True, "name": safe_name, "path": (var + safe_name) if var else safe_name}
    except Exception as e:
        return JSONResponse({"ok": False, "name": safe_name, "error": str(e)}, status_code=500)
    finally:
        try:
            await file.close()
        except Exception:
            pass


@app.api_route("/create/", methods=["GET", "POST"], response_class=HTMLResponse)
@app.api_route("/create/{var:path}", methods=["GET", "POST"], response_class=HTMLResponse)
async def create_dir(request: Request, var: str = "", ctx: dict = Depends(session_ctx)):
    var = var.split("//")[1] if "//" in var else var

    if request.method == "POST":
        form = await request.form()
        dir_name = (form.get("dir_name", "") or "").rstrip()
        ctl_wasabi.Wasabi(ctx["login"]).create_folder(var + dir_name.replace("/", "-"))

    return redirect("/files/" + var)
