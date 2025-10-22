from flask import Flask, render_template, request, send_file, redirect, session, make_response
from werkzeug.utils import secure_filename
from werkzeug.http import http_date
from datetime import datetime, timezone, timedelta
from flask_fontawesome import FontAwesome
from pathlib import Path
from cryptography.fernet import Fernet

from functools import wraps, update_wrapper

import mimetypes

from urllib.parse import quote, unquote

from controllers import ctl_wasabi

app = Flask(__name__,static_url_path='')
app.secret_key = 'LM Wasabi Drive versão 1'
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024

# FoNT AWESOME
fa = FontAwesome(app)

maxFileNameLength = 64
first_request = True

currentDirectory = ""

"FASTAPI DEPLOY https://www.digitalocean.com/community/tutorials/how-to-set-up-django-with-postgres-nginx-and-gunicorn-on-ubuntu#step-6-testing-gunicorn-s-ability-to-serve-the-project"


tp_dict = {'image': [['png', "jpg", 'svg', 'jpeg', 'png', 'gif', 'bmp', 'raw'],  'fa fa-file-image'],
           'audio': [[ 'mp3', "wav", "ogg", "mpeg", "aac", "3gpp", "3gpp2", "aiff", "x-aiff", "amr", "mpga"], 'fa fa-file-audio'], 
           'video': [['mp4', "webm", "opgg", 'flv'], 'fa fa-file-video'],
           "pdf": [['pdf'], 'fa fa-file-pdf'],
           "ppt": [['ppt', 'pptx'],  'fa fa-file-powerpoint'],
           "word": [['docx', 'doc', 'odt'],  'fa fa-file-word'],
           "excel": [['xls', 'xlsx'],  'fa fa-file-excel'],
           "txt": [['txt', 'rtf'], 'fa fa-file-text'],
           "compressed":[["zip", "rar", "7z"], 'fa fa-file-zip'],
           "code": [['css', 'scss', 'html', 'py', 'js', 'cpp'], 'fa fa-file-code']
           }

def nocache(view):
    @wraps(view)
    def no_cache(*args, **kwargs):
        response = make_response(view(*args, **kwargs))
        response.headers['Last-Modified'] = http_date(datetime.now())
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        response.headers['Cache-Control'] = 'public, max-age=0'
        return response
    return update_wrapper(no_cache, view)


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'login' not in session: 
            return redirect('/blank')
        return f(*args, **kwargs)
    return decorated_function


def getDirList(curDir:str, listdir:list):
    global maxFileNameLength, tp_dict
    all_dir = list()
    for i in [dList for dList in listdir if dList.get('isdir',False)]:
        if len(i.get('name')) > maxFileNameLength:
            dots = "..."
        else:
            dots = ""
        temp_dir = {}
        temp_dir['f'] = i['name'][0:maxFileNameLength]+dots
        temp_dir['f_url'] = i['obj']
        temp_dir['filetype'] = ''
        temp_dir['mediatype'] = ''
        temp_dir['currentDir'] = curDir
        temp_dir['system'] = i.get('system',False)
        temp_dir['isdir'] = i.get('isdir',False)
        temp_dir['icon'] = 'fa fa-folder'
        temp_dir['user'] = i.get('user','')
        temp_dir['dtm'] = ''
        temp_dir['dtm_b'] = datetime(2025,1,1)
        temp_dir['size'] = "---"
        temp_dir['size_b'] = 0
        if not i['name'] in ['.','..']:
            all_dir.append(temp_dir)

    for i in [dList for dList in listdir if not dList.get('isdir',False)]:
        icon = None
        mediatype = ''
        try:
            tp = i.get('type')
            #for file_type in tp_dict.values():
            for k, file_type in tp_dict.items():
                if tp in file_type[0]:
                    mediatype = k
                    icon = file_type[1]
                    break
            tp = "" if not tp else tp
        except Exception as e:
            pass
        if not icon:
            icon = 'fa fa-file'
        if len(i.get('name')) > maxFileNameLength:
            dots = "..."
        else:
            dots = ""
        temp_file = {}
        filesizeK = i.get('size',0) / 1024.0
        temp_file['f'] = i['name'][0:maxFileNameLength]+dots
        temp_file['filetype'] = i['type']
        temp_file['mediatype'] = mediatype
        temp_file['f_url'] = i['obj']
        temp_file['currentDir'] = curDir
        temp_file['system'] = i.get('system',False)
        temp_file['isdir'] = i.get('isdir',False)
        temp_file['icon'] = icon
        temp_file['user'] = i.get('user','')
        temp_file['dtm'] = i.get('modified', datetime.now()).strftime('%d/%m/%Y %H:%M:%S')
        temp_file['dtm_b'] = i.get('modified', datetime.now()).replace(tzinfo=timezone.utc)
        temp_file['size'] = "%.2fK" % filesizeK if filesizeK < 1000 else "%.2fM" % (filesizeK / 1024) if (filesizeK / 1024) < 1000 else "%.2fG" % (filesizeK / 1024 / 1024)
        temp_file['size_b'] = i.get('size',0)

        all_dir.append(temp_file)
    
    return sort_structure(all_dir)


def sort_structure(all_dir:list) -> list:
    sort_by_selected = session.get('sort_by_selected',0)
    sort_order = session.get('sort_order',0)

    if sort_by_selected == 0:
        if sort_order == 0:
            all_dir = sorted(all_dir, key=lambda x: x['f'].lower())
        else:
            all_dir = sorted(all_dir, key=lambda x: x['f'].lower(), reverse=True)
    elif sort_by_selected == 1:
        if sort_order == 0:
            all_dir = sorted(all_dir, key=lambda x: x['filetype'])
        else:
            all_dir = sorted(all_dir, key=lambda x: x['filetype'], reverse=True)
    elif sort_by_selected == 2:
        if sort_order == 0:
            all_dir = sorted(all_dir, key=lambda x: x['dtm_b'])
        else:
            all_dir = sorted(all_dir, key=lambda x: x['dtm_b'], reverse=True)    
    else:
        if sort_order == 0:
            all_dir = sorted(all_dir, key=lambda x: x['size_b'])
        else:
            all_dir = sorted(all_dir, key=lambda x: x['size_b'], reverse=True)     

    return all_dir


def get_mime_type(file_extension):
    # Get the MIME type for the given file extension
    mime_type, _ = mimetypes.guess_type(f"file.{file_extension}")
    return mime_type


#@app.before_first_request  # runs before FIRST request (only once)
@app.before_request
def make_session_permanent():
    global first_request
    if first_request:
        session.permanent = True
        app.permanent_session_lifetime = timedelta(hours=8)
        first_request = False


@app.route('/blank')
def blank_page():
    return render_template('blank.html')


@app.route('/login/', methods=['POST'])
@app.route('/login/<path:var>', methods=['POST'])
def loginMethod(var=""):
    return redirect('/blank')


#@app.route('/login/', methods=['GET'])
@app.route('/login/<path:var>', methods=['GET'])
def loginPost(var=""):
    try:
        key = b'5AqqPMvoZtFDilcrGCA3cIUpn10KGEBlQOcK27XD21o='
        f = Fernet(key)
        connect_cmd = f.decrypt(var).decode('utf-8')
        if connect_cmd.startswith('LMDRIVE:'):
            cmd = connect_cmd.replace('LMDRIVE:','').split('|')
            session['login'] = cmd[0]
            session['title'] = cmd[1]
            session['user'] = cmd[2]
            session['internal'] = cmd[3] if len(cmd) > 3 else 'Y'
            session['sort_by_selected'] = 0
            session['sort_order'] = 0
            _wasabi = ctl_wasabi.Wasabi(session['login'])
            _wasabi.initialize_folder() 
            return redirect('/files/')
    except:
        pass
    session.pop('login', None)
    session.pop('title', None)
    session.pop('user', None)
    session.pop('internal', None)
    session.pop('sort_by_selected', None)
    session.pop('sort_order', None)
    return render_template('blank.html')


@app.route('/external_use/', methods=['GET'])
@nocache
@login_required
def external_use():
    key = b'5AqqPMvoZtFDilcrGCA3cIUpn10KGEBlQOcK27XD21o='
    f = Fernet(key)
    token = f.encrypt(f"LMDRIVE:{session['login']}/Área_do_Cliente|{session['title']}|:{session['user']}|N".encode('utf-8')).decode('utf-8')
    return render_template('external_link.html', internal_user=session['internal'], header=session['title'], external_link=f'https://drive.linkmercado.com.br/login/{token}')


@app.route('/logout/')
def logoutMethod():
    #if('login' in session):
    session.pop('login', None)
    session.pop('title', None)
    session.pop('user', None)
    session.pop('sort_by_selected', None)
    session.pop('sort_order', None)
    return render_template('blank.html')


@app.route('/changeSort')
def toggleSort():
    coluna = request.args.get('col', "name")
    sort_order = session.get('sort_order',0)
    if coluna == "name":
        sort_by_selected = 0
    elif coluna == "filetype":
        sort_by_selected = 1
    elif coluna == "data":
        sort_by_selected = 2
    else:
        sort_by_selected = 3
    if sort_order == 0:
        sort_order = 1
    else:
        sort_order = 0

    session['sort_by_selected'] = sort_by_selected
    session['sort_order'] = sort_order

    return "OK"


@app.route('/move')
@login_required
def move():
    _from = request.args.get('from', "")
    _to = request.args.get('to',"")
    if _from and _to and _to != "null" and not (_from in _to):
        if _from.endswith('/'):
            if _to == '/':
                _to = ''
            directory_path = Path(_from)
            moveu = ctl_wasabi.Wasabi(session['login']).move_folder(_from,_to + directory_path.name + '/')
            return "/files/" + _to
        else:
            directory_path = Path(_from)
            dir = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else '/'
            moveu = ctl_wasabi.Wasabi(session['login']).move_file(origin_subfolder_path=dir, dest_subfolder_path=_to, filename=directory_path.name)
        return "OK" if moveu else "NOK"
    return "OK"


@app.route('/rename/<path:var>', methods=['GET'])
@login_required
def rename(var):
    directory_path = Path(var)
    old_name = var
    new_name = request.args.get('name',"")
    obj = request.args.get('obj',"")
    if new_name and obj and old_name not in ctl_wasabi.diretorios_sistema:
        if obj == "folder":
            moveu = ctl_wasabi.Wasabi(session['login']).move_folder(old_name,old_name.replace(directory_path.name, new_name))
        elif obj == "file":
            dir = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else '/'
            new_name += directory_path.suffix
            moveu = ctl_wasabi.Wasabi(session['login']).rename_file(subfolder_path=dir, filename=directory_path.name, new_filename=new_name)
        return "OK" if moveu else "NOK"
    return "OK"


@app.route('/files/', methods=['GET'])
@app.route('/files/<path:var>', methods=['GET'])
@nocache
@login_required
def filePage(var=""):
    breadcrumb = list()
    dir_content = getDirList(var, ctl_wasabi.Wasabi(session['login']).list_folder(var))
    cList = '' if not var else var[:-1].split('/') if var.endswith('/') else var.split('/')
    home_page = "Área do Cliente" if "Área do Cliente" in session['login'] else "Home"
    breadcrumb.append(['/files/', home_page, '/'])
    cPath = ""
    for c in cList:
        cPath += f"{c}/"
        breadcrumb.append(["/files/" + cPath, c, cPath])
    return render_template('home.html', homepage='Y', internal_user=session['internal'], header=session['title'], currentDir=var, breadcrumb=breadcrumb, all_dir=dir_content)


@app.route('/find/', methods=['POST'])
@app.route('/find/<path:var>', methods=['POST'])
@nocache
@login_required
def find(var=""):
    breadcrumb = list()
    name = request.form.get('search_name','')
    dir_content = getDirList(var,ctl_wasabi.Wasabi(session['login']).find_file(folder_path=var, searched_name=name))

    cList = '' if not var else var[:-1].split('/') if var.endswith('/') else var.split('/')
    home_page = "Área do Cliente" if "Área do Cliente" in session['login'] else "Home"
    breadcrumb.append(['/files/', home_page, '/'])
    cPath = ""
    for c in cList:
        cPath += f"{c}/"
        breadcrumb.append(["/files/" + cPath, c, cPath])
    if not var:
        var = 'Home'
    return render_template('home.html', homepage='N', mensagem=f'Resultado da busca por <u><i>{name}</i></u> em <u><strong>{var}</strong></u>', internal_user=session['internal'], header=session['title'], currentDir="", breadcrumb=breadcrumb, all_dir=dir_content)


@app.route('/', methods=['GET'])
@login_required
def homePage():
    return redirect('/files/')


@app.route('/browse/<path:var>', defaults={"download":False})
@app.route('/download/<path:var>', defaults={"download":True})
@login_required
def browseFile(var, download):
    directory_path = Path(var)
    dir = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else '/'

    f = ctl_wasabi.Wasabi(session['login']).get_object(dir, directory_path.name)
    byte_stream = f['Body']
    return send_file(byte_stream, as_attachment=download,
        download_name=directory_path.name,
        mimetype=get_mime_type(directory_path.suffix[1:]))


@app.route('/delete/<path:var>')
@login_required
def deleteFile(var):
    #if('login' not in session):
    #    return redirect('/login/download/'+var)
    directory_path = Path(var)
    dir = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else '/'
    f = ctl_wasabi.Wasabi(session['login']).delete(bucket_dir=dir, file_name=directory_path.name)
    return redirect('/files' + (dir if dir[0] == '/' else '/' + dir)  )


@app.route('/downloadFolder/')
@app.route('/downloadFolder/<path:var>')
@login_required
def downloadFolder(var=""):
    #if('login' not in session):
    #    return redirect('/login/downloadFolder/'+var)

    #os.makedirs(os.path.dirname('./downloads/'), exist_ok=True)
    wasabi = ctl_wasabi.Wasabi(session['login'])
    #download_dir = get_download_path()
    for f in getDirList(var):
        if not f['isdir']:
            directory_path = Path(f['f_url'])
            dir = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else '/'
            #ctl_wasabi.Wasabi(session['login']).download(dir, download_dir, directory_path.name)
    
            f = wasabi.get_object(dir, directory_path.name)
            byte_stream = f['Body']
            return send_file(byte_stream, as_attachment=True,
                download_name=directory_path.name,
                mimetype=get_mime_type(directory_path.suffix[1:]))

    return redirect('/files/'+var)


@app.route('/deleteFolder/')
@app.route('/deleteFolder/<path:var>')
@login_required
def deleteFolder(var=""):
    var = var.split('//')[1] if '//' in var else var
    directory_path = Path(var)
    dir = str(directory_path.parents[0]) if len(directory_path.parents) > 1 else ''
    ctl_wasabi.Wasabi(session['login']).delete_folder(var)
    return redirect('/files/'+dir)


@app.errorhandler(404)
def page_not_found(e):
    return render_template('blank.html', errorCode=404, errorText='Page Not Found'), 404


@app.route('/upload/', methods=['GET', 'POST'])
@app.route('/upload/<path:var>', methods=['GET', 'POST'])
@login_required
def uploadFile(var=""):
    files_ok, files_nok = list(), list()
    var = var.split('//')[1] if '//' in var else var
    if request.method == 'POST':
        files = request.files.getlist('files[]')
        for file in [f for f in files if f.filename != '']:
            file.filename = secure_filename(file.filename) # ensure file name is secure
            w = ctl_wasabi.Wasabi(session['login']).put_object(bucket_dir=var, obj_name=file.filename.rstrip(), obj_data=file.read(), user=session['user'])
            if w['ResponseMetadata']['HTTPStatusCode'] in (200,204):
                files_ok.append(file.filename)
            else:
                files_nok.append(file.filename)
    return render_template('uploadsuccess.html', internal_user=session['internal'], header=session['title'], fileOK=files_ok, fileNOK=files_nok, href=quote("/files/"+var))


@app.route('/create/', methods=['GET','POST'])
@app.route('/create/<path:var>', methods=['GET','POST'])
@login_required
def createdir(var=""):
    var = var.split('//')[1] if '//' in var else var
    if request.method == 'POST':
        dir_name = request.form.get('dir_name','').rstrip()
        ctl_wasabi.Wasabi(session['login']).create_folder(var + dir_name.replace('/','-'))
    return redirect('/files/'+var)    


if __name__ == '__main__':
    app.run(host='0.0.0.0')